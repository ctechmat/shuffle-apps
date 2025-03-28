import yaml
import jsonpickle

from shuffle_sdk import AppBase

class Yamltojson(AppBase):
    __version__ = "1.0.0"
    app_name = "Yaml to Json"

    def __init__(self, redis, logger, console_logger=None):
        """
        Setup Redis and logging.
        :param redis:
        :param logger:
        :param console_logger:
        """
        super().__init__(redis, logger, console_logger)

    def yaml_to_json(self, file_id):
        file_path = {
            "success": False,
        }

        # Obtain the file with the id provided
        if isinstance(file_id, dict) and "data" in file_id:
            file_path = file_id
        else:
            file_path = self.get_file(file_id)

        if file_path["success"] == False:
            return {
                "success": False,
                "reason": "Couldn't get file with ID %s" % file_id
            }

        # Check if the content of the file is in YAML and try to decode it
        try:
            file_data = file_path["data"].decode("utf-8")
            yaml_content = yaml.safe_load(file_data)

            json_content = jsonpickle.dumps(yaml_content, indent=2)

            return {"success": True, "message": json_content}
        
        except yaml.YAMLError as e:
            return {"success": False, "reason": f"Failed to parse YAML: {e}"}
        except Exception as e:
            return {"success": False, "reason": f"An error occurred: {e}"}

if __name__ == "__main__":
    Yamltojson.run()
