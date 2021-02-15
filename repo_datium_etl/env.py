import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

#ROOT_DIR = Path(__file__).parent.parent

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()

# load up the entries as environment variables
load_dotenv(dotenv_path)

project_dir = Path(__file__).parent.parent
data_dir = os.path.join(project_dir,'data/raw/')
processed_data_dir = os.path.join(project_dir,'data/processed/')
interim_data_dir = os.path.join(project_dir,'data/interim/')
raw_data_file = os.path.join(data_dir, os.environ.get("data_file_csv"))
raw_data_file_json = os.path.join(data_dir, os.environ.get("data_file_json"))

policy_file = os.path.join(project_dir,'policy/pii_policy.yaml')

#print(ROOT_DIR)

