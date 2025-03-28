import os
import subprocess
import logging
import json
import yaml
from shuffle_sdk import AppBase

class Sigmav2(AppBase):
    __version__ = "1.0.0"
    app_name = "Sigmav2"

    def __init__(self, redis, logger, console_logger=None):
        super().__init__(redis, logger, console_logger)

    def _save_file(self, file_id):
        if isinstance(file_id, dict) and "data" in file_id:
            file_path = file_id
        else:
            file_path = self.get_file(file_id)

        filename = file_path["filename"]
        file_content = file_path["data"].decode("utf-8")

        if not filename or not file_content:
            return None, f"Invalid file data for file ID {file_id}"

        basedir = "siemrule"
        os.mkdir(basedir)

        try:
            self.logger.info(f"Saving file: {filename} to {basedir}/{filename}")
            with open(f"{basedir}/{filename}", "wb+") as tmp:
                if isinstance(file_content, str):
                    file_content = file_content.encode('utf-8')
                tmp.write(file_content)
            self.logger.info(f"File {filename} saved successfully.")
            return basedir, filename
        except Exception as e:
            self.logger.error(f"Error while saving the file: {e}")
            return None, f"Error while saving the file: {e}"

    def _clean_data(self, data):
        """Traverse les données et convertit les UUID en chaînes de caractères."""
        if isinstance(data, dict):
            return {key: self._clean_data(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._clean_data(item) for item in data]
        elif isinstance(data, UUID):
            return str(data)  # Convertir UUID en chaîne
        else:
            return data

    def _run_conversion(self, backend, format, file_path, filename):
        if not os.path.exists(file_path):
            self.logger.error(f"File {filename} does not exist in {file_path}.")
            return {"success": False, "reason": f"File {filename} does not exist in {file_path}."}

        code = ["sigma", "convert", "--without-pipeline", "-t", backend, file_path]
        if format:
            code.extend(["-f", format])

        self.logger.info(f"Running command: {' '.join(code)}")

        try:
            result = subprocess.run(
                code,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            self.logger.info(f"Command Output: {result.stdout}")
            query_result = result.stdout.strip()

            if format == "default":
                os.remove(file_path)
                self.logger.info(f"File {filename} deleted successfully.")
                return {"success": True, "query": query_result}

            elif format == "ruler":
                parsed_data = yaml.safe_load(query_result)
                expr = parsed_data.get("groups", [])[0].get("rules", [])[0].get("expr", "")
                os.remove(file_path)
                self.logger.info(f"File {filename} deleted successfully.")
                return {"success": True, "query": expr}

            elif format == "savedsearches":
                search_query = next(
                    (line.split("search = ", 1)[1].strip() for line in query_result.splitlines() if line.startswith("search = ")),
                    None
                )
                os.remove(file_path)
                self.logger.info(f"File {filename} deleted successfully.")
                
                return (
                    {"success": True, "query": search_query} if search_query 
                    else {"success": False, "reason": "No 'search' expression found in the file."}
                )

            else:
                os.remove(file_path)
                self.logger.info(f"File {filename} deleted successfully.")
                return result.stdout

        except subprocess.CalledProcessError as e:
            self.logger.error(f"Command failed with error: {e.stderr}")
            return {"success": False, "reason": e.stderr}
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
                self.logger.info(f"File {filename} deleted in finally block.")

    def carbon_black(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("carbon_black", format, f"{basedir}/{filename}", filename)

    def cortex_xdr(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("cortex_xdr", format, f"{basedir}/{filename}", filename)

    def datadog(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("datadog", format, f"{basedir}/{filename}", filename)

    def eql(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("eql", format, f"{basedir}/{filename}", filename)

    def elastalert(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("elastalert", format, f"{basedir}/{filename}", filename)

    def esql(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("esql", format, f"{basedir}/{filename}", filename)

    def kusto(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("kusto", format, f"{basedir}/{filename}", filename)

    def logpoint(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("logpoint", format, f"{basedir}/{filename}", filename)

    def log_scale(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("log_scale", format, f"{basedir}/{filename}", filename)

    def loki(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("loki", format, f"{basedir}/{filename}", filename)

    def lucene(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("lucene", format, f"{basedir}/{filename}", filename)

    def panther(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("panther", format, f"{basedir}/{filename}", filename)

    def quickwit(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("quickwit", format, f"{basedir}/{filename}", filename)

    def secops(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("secops", format, f"{basedir}/{filename}", filename)

    def sentinel_one(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("sentinel_one", format, f"{basedir}/{filename}", filename)

    def sentinel_one_pq(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("sentinel_one_pq", format, f"{basedir}/{filename}", filename)

    def sqlite(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("sqlite", format, f"{basedir}/{filename}", filename)

    def splunk(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("splunk", format, f"{basedir}/{filename}", filename)

    def tql(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("tql", format, f"{basedir}/{filename}", filename)

    def uberagent(self, file_id, format=None):
        basedir, filename = self._save_file(file_id)
        if not basedir:
            return {"success": False, "reason": filename}
        return self._run_conversion("uberagent", format, f"{basedir}/{filename}", filename)

if __name__ == "__main__":
    Sigmav2.run()
