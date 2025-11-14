import json, os, sys
import importlib.util

os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

def main():
    if len(sys.argv) != 3:
        print("Usage: python local_runner.py <lambda_app_path> <event_json_path>")
        print("Example:")
        print("  python local_runner.py src/lambda/app.py events/event.json")
        sys.exit(1)
    
    handler_path = sys.argv[1]
    event_json_path = sys.argv[2]
    
    # Load the event
    with open(event_json_path, 'r') as f:
        event = json.load(f)
    
    # Load and invoke the handler
    handler = load_handler(handler_path)
    
    print(f"Invoking {handler_path} with event from {event_json_path}")
    print("-" * 60)
    
    result = handler(event, None)
    
    print("-" * 60)
    print("Result:")
    print(json.dumps(result, indent=2))
    
    return result


def load_handler(lambda_app_path:str):
    spec = importlib.util.spec_from_file_location("lambda_app", lambda_app_path)
    if spec is None:
        raise ImportError(f"Couldn't find spec from file location at {lambda_app_path}")
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None or module is None:
        raise ImportError(f"Couldn't resolve module from spec at {lambda_app_path}")
    spec.loader.exec_module(module)
    
    return getattr(module, "lambda_handler")


if __name__ == "__main__":
    main()