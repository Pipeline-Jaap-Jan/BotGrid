import logging
import os
import time
from threading import Lock

import shotgun_api3 as shotgun
from dotenv import load_dotenv
from flask import Flask, abort, request
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

app = Flask(__name__)

load_dotenv()

slack_token = os.getenv("SLACK_TOKEN")
if not slack_token:
    raise ValueError("SLACK_TOKEN is not set in the environment. Please check your .env file.")
client = WebClient(token=slack_token)

shotgrid_connection = shotgun.Shotgun(
    "https://nfa.shotgunstudio.com", script_name="toSlack_export", api_key=os.getenv("SHOTGRID_API_KEY")
)

# =============================================================
"""LET OP WERKT NIET GEBRUIK v27"""
# =============================================================


class Throttler:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = 0
        self.lock = Lock()
        self.last_reset = time.time()

    def throttle(self):
        with self.lock:
            current_time = time.time()
            if current_time - self.last_reset > self.period:
                self.calls = 0
                self.last_reset = current_time
            if self.calls < self.max_calls:
                self.calls += 1
                return True
            return False


throttler = Throttler(max_calls=5, period=10)


def send_slack_message(slack_user_id, text):
    """Sends a Slack message to a specific user."""
    try:
        response = client.chat_postMessage(channel=slack_user_id, text=text)
        logging.info(f"Message sent successfully at {response['ts']} to {slack_user_id}")
    except SlackApiError as e:
        logging.error(f"Error sending message: {e.response['error']}")


def get_shotgrid_user_email(user_id):
    """Query ShotGrid for the user's email using their user_id."""
    user = shotgrid_connection.find_one("HumanUser", [["id", "is", user_id]], ["email"])
    if user and "email" in user:
        return user["email"]
    return None


def find_slack_user_by_email(email):
    """Use Slack's API to find a Slack user by email address."""
    try:
        response = client.users_lookupByEmail(email=email)
        slack_user_id = response["user"]["id"]
        return slack_user_id
    except SlackApiError as e:
        logging.error(f"Error finding Slack user by email: {e.response['error']}")
        return None


def get_assigned_users_from_tasks(shot_id):
    """Query ShotGrid for all tasks associated with a shot and return assigned users' email addresses,
    along with shot, sequence, and project names."""
    shot_details = shotgrid_connection.find_one(
        "Shot", [["id", "is", shot_id]], ["code", "project.Project.name", "sg_sequence.Sequence.code"]
    )

    if not shot_details:
        logging.error(f"Could not retrieve shot details for Shot ID: {shot_id}")
        return None, None, None, None

    shot_name = shot_details.get("code", "Unknown Shot")
    sequence_name = shot_details.get("sg_sequence.Sequence.code", "Unknown Sequence")
    project_name = shot_details.get("project.Project.name", "Unknown Project")

    tasks = shotgrid_connection.find(
        "Task", [["entity", "is", {"type": "Shot", "id": shot_id}]], ["task_assignees", "step.Step.short_name"]
    )

    assigned_users = {}
    for task in tasks:
        task_assignees = task.get("task_assignees", [])
        pipeline_step = task.get("step.Step.short_name", "Unknown Step")

        if task_assignees:
            for assignee in task_assignees:
                user_data = shotgrid_connection.find_one("HumanUser", [["id", "is", assignee["id"]]], ["email"])
                if user_data:
                    user_email = user_data["email"]
                    if pipeline_step not in assigned_users:
                        assigned_users[pipeline_step] = []
                    assigned_users[pipeline_step].append(user_email)

    return assigned_users, shot_name, sequence_name, project_name


def get_assigned_users_from_asset_tasks(asset_id):
    """Query ShotGrid for all tasks associated with an asset and return assigned users' email addresses,
    along with asset and project names."""
    asset_details = shotgrid_connection.find_one("Asset", [["id", "is", asset_id]], ["code", "project.Project.name"])

    if not asset_details:
        logging.error(f"Could not retrieve asset details for Asset ID: {asset_id}")
        return None, None, None

    asset_name = asset_details.get("code", "Unknown Asset")
    project_name = asset_details.get("project.Project.name", "Unknown Project")

    tasks = shotgrid_connection.find(
        "Task", [["entity", "is", {"type": "Asset", "id": asset_id}]], ["task_assignees", "step.Step.short_name"]
    )

    assigned_users = {}
    for task in tasks:
        task_assignees = task.get("task_assignees", [])
        pipeline_step = task.get("step.Step.short_name", "Unknown Step")

        if task_assignees:
            for assignee in task_assignees:
                user_data = shotgrid_connection.find_one("HumanUser", [["id", "is", assignee["id"]]], ["email"])
                if user_data:
                    user_email = user_data["email"]
                    if pipeline_step not in assigned_users:
                        assigned_users[pipeline_step] = []
                    assigned_users[pipeline_step].append(user_email)

    return assigned_users, asset_name, project_name


def get_assigned_users_from_version_tasks(version_id):
    """Query ShotGrid for all tasks associated with a version, find the linked shot,
    and return assigned users' email addresses, along with version and project names."""

    version_details = shotgrid_connection.find_one(
        "Version", [["id", "is", version_id]], ["code", "project.Project.name", "sg_shot.Shot.code"]
    )

    if not version_details:
        logging.error(f"Could not retrieve version details for Version ID: {version_id}")
        return None, None, None

    version_name = version_details.get("code", "Unknown Version")
    project_name = version_details.get("project.Project.name", "Unknown Project")

    shot_id = version_details.get("sg_shot.Shot.code")

    if not shot_id:
        logging.warning(f"No shot linked to Version ID: {version_id}")
        return {}, version_name, project_name

    assigned_users_by_step, shot_name, sequence_name, project_name = get_assigned_users_from_tasks(shot_id)

    if not assigned_users_by_step:
        logging.warning(f"No assigned users found for Shot ID: {shot_id}")
        return {}, version_name, project_name

    return assigned_users_by_step, version_name, project_name


def get_attachments_ids_from_note_id(node_id: int) -> list:
    """Searches ShotGrid database for attachments that are linked to a note and returns their IDs.

    Args:
        node_id: The ID of the note to search for.

    Returns:
        A list of attachment IDs.
    """
    filters = [
        [
            "id",
            "is",
            node_id,
        ]
    ]
    fields = ["id", "attachments"]
    note = shotgrid_connection.find_one("Note", filters, fields)

    if not note or "attachments" not in note:
        logging.warning(f"No attachments found for Note ID: {node_id}")
        return []

    attachment_ids = [attachment["id"] for attachment in note["attachments"]]

    return attachment_ids


def get_file_url_from_attachment_id(attachment_id: int) -> str:
    """Searches ShotGrid database for attachment that matches attachment ID and returns its file path.

    Args:
        attachment_id: The ID of the attachment to search for.

    Returns:
        The file URL of the attachment, or an empty string if not found.
    """
    filters = [
        [
            "id",
            "is",
            attachment_id,
        ]
    ]
    fields = ["this_file"]
    attachment = shotgrid_connection.find_one("Attachment", filters, fields)

    if attachment and "this_file" in attachment:
        return attachment["this_file"]["url"]

    logging.warning(f"Attachment ID {attachment_id} not found.")
    return ""


def send_message_to_assigned_users(assigned_users_by_step, shot_name, sequence_name, project_name, message_content):
    """Send a Slack message to each assigned user found in the ShotGrid tasks, including shot, sequence, and project details."""
    for step, users in assigned_users_by_step.items():
        for email in users:
            slack_user_id = find_slack_user_by_email(email)
            if slack_user_id:
                try:
                    personalized_message = (
                        f"In {project_name}|{sequence_name}|{shot_name}|{step}\n" f"{message_content}"
                    )
                    send_slack_message(slack_user_id, personalized_message)
                    logging.info(f"Message sent to {email} (Slack ID: {slack_user_id}) for step {step}")
                except SlackApiError as e:
                    logging.error(f"Error sending message to {email}: {e.response['error']}")
            else:
                logging.warning(f"Could not find Slack user for email: {email}")


@app.route("/webhook", methods=["POST"])
def webhook():
    if request.method == "POST":
        if request.json:
            print("Received JSON data:")
            print(request.json)

            try:
                event_data = request.json.get("data", {})
                entity_type = event_data.get("meta", {}).get("entity_type")
                entity_id = event_data.get("meta", {}).get("entity_id")
                operation = event_data.get("operation")
                event_type = event_data.get("event_type")

                if entity_type == "Shot":
                    return handle_shot_event(event_data)
                if entity_type == "Note" and event_type == "Shotgun_Note_New" and operation == "create":
                    return handle_note_event(event_data)
                if entity_type == "Reply" and event_type == "Shotgun_Reply_New" and operation == "create":
                    return handle_reply_event(event_data)
                if entity_type == "Task" and event_type == "Shotgun_Task_Change" and operation == "update":
                    return handle_task_assignment_event(event_data)
                logging.warning(f"Unsupported entity type or event: {entity_type}, {event_type}")
                return "Entity type or event not supported", 400

            except Exception as e:
                logging.error(f"Error processing the request: {e!s}")
                return "Error processing data", 500
        else:
            logging.error("No JSON received or invalid data.")
            return "Invalid data format", 400
    else:
        abort(400)


STATUS_DESCRIPTIONS = {
    "wtg": "Waiting to Start",
    "rdy": "Ready to Start",
    "ip": "In Progress",
    "qc": "Quality Control",
    "hld": "On Hold",
    "omt": "Omit",
    "pla": "Plate",
    "extrev": "Pending External Review",
    "prop": "Proposed Final",
    "rfd": "Ready for Delivery",
    "fin": "Final",
    "rev": "Pending Review",
    "rc": "Requires Changes",
}


def handle_shot_event(event_data):
    """Process Shot-related events."""
    entity_id = event_data.get("meta", {}).get("entity_id")
    attribute_name = event_data.get("meta", {}).get("attribute_name")
    old_value = event_data.get("meta", {}).get("old_value")
    new_value = event_data.get("meta", {}).get("new_value")

    old_description = STATUS_DESCRIPTIONS.get(old_value, old_value)
    new_description = STATUS_DESCRIPTIONS.get(new_value, new_value)

    message_content = f"A shot status has been changed from '{old_description}' to '{new_description}'."

    assigned_users_by_step, shot_name, sequence_name, project_name = get_assigned_users_from_tasks(entity_id)

    if assigned_users_by_step:
        for step, users in assigned_users_by_step.items():
            personalized_message = f"{message_content}"
            send_message_to_assigned_users({step: users}, shot_name, sequence_name, project_name, personalized_message)

        return "success", 200
    logging.warning(f"No assigned users found for Shot ID: {entity_id}")
    return "No assigned users found", 404


def handle_note_event(event_data):
    """Process Note-related events."""
    note_id = event_data.get("meta", {}).get("entity_id")
    logging.info(f"New note created with ID: {note_id}")

    note_details = shotgrid_connection.find_one(
        "Note",
        [["id", "is", note_id]],
        ["content", "note_links", "created_by.HumanUser.email", "created_by.HumanUser.name"],
    )

    if not note_details:
        logging.error(f"Could not retrieve details for Note ID: {note_id}")
        return "Note details not found", 404

    note_content = note_details.get("content", "No content")
    note_links = note_details.get("note_links", [])
    created_by_email = note_details.get("created_by.HumanUser.email", "unknown")
    created_by_name = note_details.get("created_by.HumanUser.name", "unknown user")

    if not note_links:
        logging.warning(f"No linked entities found for Note ID: {note_id}")
        return "No linked entities found", 404

    linked_entity = note_links[0]
    linked_entity_type = linked_entity["type"]
    linked_entity_id = linked_entity["id"]

    message_content = f"{created_by_name} added a note:\n{note_content}"

    time.sleep(3)

    attachment_ids = get_attachments_ids_from_note_id(note_id)
    annotated_frame_url = ""

    for attachment_id in attachment_ids:
        file_url = get_file_url_from_attachment_id(attachment_id)
        if file_url:
            annotated_frame_url = file_url
            break

    if annotated_frame_url:
        message_content += f"\nAnnotated Frame: {annotated_frame_url}"

    if linked_entity_type == "Shot":
        assigned_users_by_step, shot_name, sequence_name, project_name = get_assigned_users_from_tasks(linked_entity_id)
        if assigned_users_by_step:
            for step, users in assigned_users_by_step.items():
                send_message_to_assigned_users({step: users}, shot_name, sequence_name, project_name, message_content)
            return "success", 200
        logging.warning(f"No assigned users found for linked Shot ID: {linked_entity_id}")
        return "No assigned users found for linked Shot", 404

    if linked_entity_type == "Asset":
        assigned_users_by_step, asset_name, project_name = get_assigned_users_from_asset_tasks(linked_entity_id)
        if assigned_users_by_step:
            for step, users in assigned_users_by_step.items():
                send_message_to_assigned_users({step: users}, asset_name, "N/A", project_name, message_content)
            return "success", 200
        logging.warning(f"No assigned users found for linked Asset ID: {linked_entity_id}")
        return "No assigned users found for linked Asset", 404

    if (
        linked_entity_type == "Version"
        or linked_entity_type == "Note, Shotgun_Note_Change"
        or linked_entity_type == "Playlist"
    ):
        assigned_users_by_step, version_name, project_name = get_assigned_users_from_version_tasks(linked_entity_id)
        if assigned_users_by_step:
            for step, users in assigned_users_by_step.items():
                send_message_to_assigned_users({step: users}, version_name, "N/A", project_name, message_content)
            return "success", 200
        logging.warning(f"No assigned users found for linked Version ID: {linked_entity_id}")
        return "No assigned users found for linked Version", 404

    logging.warning(f"Unsupported linked entity type: {linked_entity_type} for Note ID: {note_id}")
    return "Unsupported linked entity type", 400


def handle_task_assignment_event(event_data):
    """Process Task assignment-related events."""
    entity_id = event_data.get("meta", {}).get("entity_id")
    attribute_name = event_data.get("meta", {}).get("attribute_name")

    if attribute_name != "task_assignees":
        logging.info(f"Change in Task ID {entity_id} is not related to task assignments.")
        return "Not a task assignment event", 200

    added_assignees = event_data.get("meta", {}).get("added", [])
    removed_assignees = event_data.get("meta", {}).get("removed", [])

    task_details = shotgrid_connection.find_one("Task", [["id", "is", entity_id]], ["entity", "step.Step.short_name"])

    if not task_details:
        logging.error(f"Could not retrieve details for Task ID: {entity_id}")
        return "Task details not found", 404

    linked_shot = task_details.get("entity")
    step_name = task_details.get("step.Step.short_name", "Unknown Step")

    if linked_shot and linked_shot["type"] == "Shot":
        shot_id = linked_shot["id"]
        shot_name = linked_shot["name"]

        shot_details = shotgrid_connection.find_one(
            "Shot", [["id", "is", shot_id]], ["project.Project.name", "sg_sequence.Sequence.code"]
        )
        project_name = shot_details.get("project.Project.name", "Unknown Project")
        sequence_name = shot_details.get("sg_sequence.Sequence.code", "Unknown Sequence")

        for assignee in added_assignees:
            user_email = get_shotgrid_user_email(assignee["id"])
            if user_email:
                slack_user_id = find_slack_user_by_email(user_email)
                if slack_user_id:
                    message_content = (
                        f"In {project_name}|{sequence_name}|{shot_name}|{step_name}\n"
                        f"You have been assigned to a task."
                    )
                    send_slack_message(slack_user_id, message_content)
                else:
                    logging.warning(f"Slack user not found for email: {user_email}")

        for removed_assignee in removed_assignees:
            user_email = get_shotgrid_user_email(removed_assignee["id"])
            if user_email:
                slack_user_id = find_slack_user_by_email(user_email)
                if slack_user_id:
                    message_content = (
                        f"In {project_name}|{sequence_name}|{shot_name}|{step_name}\n"
                        f"You have been removed from a task."
                    )
                    send_slack_message(slack_user_id, message_content)
                else:
                    logging.warning(f"Slack user not found for email: {user_email}")
    else:
        logging.info(f"Task {entity_id} is not linked to a Shot.")
        return "Not linked to a Shot", 200

    return "success", 200


STATUS_DESCRIPTIONS = {
    "wtg": "Waiting to Start",
    "rdy": "Ready to Start",
    "ip": "In Progress",
    "hld": "On Hold",
    "omt": "Omit",
    "ia": "Internally Approved",
    "rev": "Pending Review",
    "nupt": "New update",
    "rc": "Requires Changes",
    "lib": "Library",
    "prop": "Proposed Final",
    "qc": "Quality Control",
    "fin": "Final",
}


def handle_asset_event(event_data):
    """Process Asset-related events."""
    entity_id = event_data.get("meta", {}).get("entity_id")
    attribute_name = event_data.get("meta", {}).get("attribute_name")
    old_value = event_data.get("meta", {}).get("old_value")
    new_value = event_data.get("meta", {}).get("new_value")

    old_description = STATUS_DESCRIPTIONS.get(old_value, old_value)
    new_description = STATUS_DESCRIPTIONS.get(new_value, new_value)

    message_content = f"An asset's status has been changed from '{old_description}' to '{new_description}'."

    assigned_users_by_step, asset_name, project_name = get_assigned_users_from_asset_tasks(entity_id)

    if assigned_users_by_step:
        for step, users in assigned_users_by_step.items():
            personalized_message = f"{message_content}"
            send_message_to_assigned_users({step: users}, asset_name, "N/A", project_name, personalized_message)

        logging.info(f"Notification sent for Asset ID: {entity_id}")
        return "success", 200
    logging.warning(f"No assigned users found for Asset ID: {entity_id}")
    return "No assigned users found", 404


def handle_reply_event(event_data):
    """Process Reply-related events."""
    reply_id = event_data.get("meta", {}).get("entity_id")
    logging.info(f"New reply created with ID: {reply_id}")

    reply_details = shotgrid_connection.find_one(
        "Reply",
        [["id", "is", reply_id]],
        ["content", "note.Note.content", "note.Note.note_links", "created_by.HumanUser.email"],
    )

    if not reply_details:
        logging.error(f"Could not retrieve details for Reply ID: {reply_id}")
        return "Reply details not found", 404

    reply_content = reply_details.get("content", "No content")
    note_content = reply_details.get("note.Note.content", "No associated note content")
    note_links = reply_details.get("note.Note.note_links", [])
    created_by_email = reply_details.get("created_by.HumanUser.email", "unknown")

    slack_user_id = find_slack_user_by_email(created_by_email) if created_by_email else None

    if note_links:
        linked_entity = note_links[0]
        if linked_entity["type"] == "Shot":
            linked_shot_id = linked_entity["id"]

            assigned_users_by_step, shot_name, sequence_name, project_name = get_assigned_users_from_tasks(
                linked_shot_id
            )

            if assigned_users_by_step:
                message_content = f"A new Reply has been created:\n{reply_content}\n" f"Related Note: {note_content}"

                send_message_to_assigned_users(
                    assigned_users_by_step, shot_name, sequence_name, project_name, message_content
                )
                return "success", 200
            logging.warning(f"No assigned users found for linked Shot ID: {linked_shot_id}")
            return "No assigned users found for linked entity", 404

    if slack_user_id:
        message_content = f"A new Reply has been created by you:\n{reply_content}\n" f"Related Note: {note_content}"
        send_slack_message(slack_user_id, message_content)
        return "success", 200
    logging.warning(f"No linked entities or Slack user found for Reply ID: {reply_id}")
    return "No users found for notification", 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=19132, debug=True)
