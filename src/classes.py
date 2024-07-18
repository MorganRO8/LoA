import os

class ScrapeSettings():
    def __init__(self):
        self.scrape_pubmed      = False
        self.scrape_arxiv       = False
        self.scrape_scienceopen = False
        self.scrape_unpaywall   = False
        self.scrape_custom_db   = False
        self.retmax             = 100
        self.base_url           = ""
        self.email              = ""
    def _parse_from_json(self,json):
        for key, val in json.items():
            if key.lower() == "pubmed":
                self.scrape_pubmed = bool(val.lower() == "y")
            elif key.lower() == "arxiv":
                self.scrape_arxiv = bool(val.lower() == "y")
            elif key.lower() == "scienceopen":
                self.scrape_scienceopen = bool(val.lower() == "y")
            elif key.lower() == "unpaywall":
                self.scrape_unpaywall = bool(val.lower() == "y")
            elif key.lower() == "customdb":
                self.scrape_custom_db = bool(val.lower() == "y")
            elif key.lower() == "retmax":
                self.retmax = int(val)
            elif key.lower() == "base_url":
                self.base_url = str(val)
            elif key.lower() == "email":
                self.email = str(val)
            else:
                print(f"Scrape setting '{key}' not recognized. \n")

class ExtractSettings():
    def __init__(self):
        self.max_retries = 3
        self.ollama_url  = "http://localhost:11434"
        self.user_instructions = "Explain the extraction task here"
    def _parse_from_json(self,json):
        for key, val in json.items():
            if key.lower() == "max_retries":
                self.max_retries = int(val)
            elif key.lower() == "ollama_url":
                self.ollama_url = str(val)
            elif key.lower() == "user_instructions":
                self.user_instructions = str(val)
            else:
                print(f"Extract setting '{key}' not recognized. \n")

class FileSettings():
    def __init__(self):
        self.json = "automatic.json"
        self.schema = "schema.pkl"
        self.csv = "results_output.csv"
        self.log = "LoA.log"
    def _parse_from_json(self,json):
        for key,val in json.items():
            if key.lower() == "schema_file":
                self.schema = os.path.join(os.getcwd(), 'dataModels', str(val) )
            elif key.lower() == "log":
                self.log = str(val)
            elif key.lower() == "results_csv":
                self.csv = str(val)
            else:
                print(f"Files setting '{key}' not recognized. \n")

class JobSettings(): ## Contains subsettings as well for each of the job types.
    def __init__(self):
        self.scrape = ScrapeSettings()
        self.extract = ExtractSettings()
        self.files = FileSettings()
        self.auto = False
        self.run_scrape = False
        self.run_extract= False
        self.concurrent = False
        self.def_search_terms = []
        self.maybe_search_terms = []
        self.model_name_version = "mistral:7b-instruct-v0.2-q8_0"
        self.model_name = "mistral"
        self.model_version = "7b-instruct-v0.2-q8_0"

    def _update_model_name_version(self,model_name_version):
        if ":" not in model_name_version:
            model_name_version += ":latest"
        self.model_name,self.model_version = model_name_version.split(":")
        self.model_name_version = f"{self.model_name}:{self.model_version}"

    def _parse_from_json(self,json):
        for key,val in json.items():
            if key.lower() == "def_search_terms":
                if type(val) == list:
                    self.def_search_terms = val
                elif type(val) == str:
                    self.def_search_terms = val.split(",")
            elif key.lower() == "maybe_search_terms":
                if type(val) == list:
                    self.maybe_search_terms = val
                elif type(val) == str:
                    self.maybe_search_terms = val.split(",")
            elif key.lower() == "model_name_version":
                self._update_model_name_version(val)
            elif key.lower() == "concurrent":
                self.concurrent = bool(val.lower() == "y")
            else:
                print(f"JSON key '{key}' not recognized.")
