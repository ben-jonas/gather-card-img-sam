import csv, json, os, sys
import importlib.util
from io import StringIO

os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
INVOKE_HANDLER_CMD = "invoke_handler"
CREATE_BATCH_IN_PROGRESS_CMD = "create_batch_in_progress"
ALL_COMMANDS = [INVOKE_HANDLER_CMD, CREATE_BATCH_IN_PROGRESS_CMD]

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ALL_COMMANDS:
        print(f"Available commands: {str(ALL_COMMANDS)}")
        sys.exit(1)
    elif sys.argv[1] == INVOKE_HANDLER_CMD:
        if len(sys.argv) != 4:
            print("Usage: python local_runner.py invoke_handler <lambda_app_path> <event_json_path>")
            print("Example:")
            print("  python local_runner.py src/lambda/app.py events/event.json")
            sys.exit(1)
        return invoke_handler()
    elif sys.argv[1] == CREATE_BATCH_IN_PROGRESS_CMD:
        if len(sys.argv) != 5:
            print("Usage: python local_runner.py create_batch_in_progress <workspace_folder> <event_json_path> <uuid_for_dynamo>")
            print("Example:")
            print("  python local_runner.py src/lambda/app.py events/event.json")
            sys.exit(1)
        return create_batch_in_progress()
    else:
        print("Unrecognized command (this should be unreachable)")
        sys.exit(1)

def create_batch_in_progress():
    os.environ["CARD_IMG_FETCH_QUEUE"] = "dontworryaboutit" # unused, but module won't load without it
    workspace_folder = sys.argv[2]
    event_json_path = sys.argv[3]
    batch_id = sys.argv[4]

    module_location = workspace_folder + "/src/cardimg_add_batch/app.py"
    create_dynamo_record = load_handler_at_entrypoint(module_location, "create_dynamo_record")

    with open(event_json_path, 'r') as f:
        event = json.load(f)
        csv_reader = csv.DictReader(StringIO(event['body']))
        csv_data = list(csv_reader)

    create_dynamo_record(csv_data, batch_id)

def invoke_handler():
    handler_path = sys.argv[2]
    event_json_path = sys.argv[3]
    
    # Load the event
    with open(event_json_path, 'r') as f:
        event = json.load(f)
    
    # Load and invoke the handler
    handler = load_handler_at_entrypoint(handler_path, "lambda_handler")
    
    print(f"Invoking {handler_path} with event from {event_json_path}")
    print("-" * 60)
    
    result = handler(event, None)
    
    print("-" * 60)
    print("Result:")
    print(json.dumps(result, indent=2))
    
    return result

def load_handler_at_entrypoint(lambda_app_path:str, entrypoint:str):
    spec = importlib.util.spec_from_file_location("lambda_app", lambda_app_path)
    if spec is None:
        raise ImportError(f"Couldn't find spec from file location at {lambda_app_path}")
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None or module is None:
        raise ImportError(f"Couldn't resolve module from spec at {lambda_app_path}")
    spec.loader.exec_module(module)
    
    return getattr(module, entrypoint)


if __name__ == "__main__":
    main()