from dagster import execute_pipeline, pipeline, solid
import os
from pymongo import MongoClient
import git
import os
import shutil
import subprocess

client = MongoClient()
db = client.cncf
cncf_items = db.items

# Get one item from mongo if not executed
@solid
def get_cncf_item(context):
    mongo_item = cncf_items.find_one({"processed": False})
    return mongo_item


@solid
def clone_repo(context, mongo_item):
    path = os.path.join(os.getcwd(), "pipeline_ws")
    repo_path = os.path.join(path, "repos")
    folder_name = mongo_item['repo_url'].split("/")[-1].split(".")[0]
    context.log.info(folder_name)
    context.log.info(os.path.join(repo_path, folder_name))
    if os.path.exists(os.path.join(repo_path, folder_name)):
        shutil.rmtree(os.path.join(repo_path, folder_name))
    git.Git(repo_path).clone(mongo_item['repo_url'])
    return mongo_item

@solid
def prepare_working_space(context, mongo_item):
    path = os.path.join(os.getcwd(), "pipeline_ws")
    video_in_process_path = os.path.join(path, "video_in_process")
    folder_name = mongo_item['repo_url'].split("/")[-1].split(".")[0]
    if not os.path.exists(video_in_process_path):
        os.mkdir(video_in_process_path)
    return mongo_item

@solid
def run_gource(context, mongo_item):
    name = mongo_item['name']
    path = os.path.join(os.getcwd(), "pipeline_ws")
    repo_path = os.path.join(path, "repos")
    video_in_process_path = os.path.join(path, "video_in_process")
    folder_name = mongo_item['repo_url'].split("/")[-1].split(".")[0]
    item_path = os.path.join(repo_path, folder_name)
    output_path = os.path.join(os.path.join(path, video_in_process_path), name+".mp4")
    logo_path = os.path.join(os.path.join(path, "logos"), mongo_item['logo'])
    cmd_string = 'gource --hide filenames,dirnames,usernames,progress,mouse --key --title "' + name + ' Development History" --font-size 20 --seconds-per-day 0.1 --auto-skip-seconds 1 -1920x1080 --logo ' + logo_path + ' --path ' + item_path + ' -o - | ffmpeg -y -r 60 -f image2pipe -vcodec ppm -i - -vcodec libx264 -preset ultrafast -pix_fmt yuv420p -crf 1 -threads 0 -bf 0 ' + output_path
    os.system(cmd_string)


@pipeline
def cncf_yt_pipeline():
    cncf_item = get_cncf_item()
    cncf_item = clone_repo(cncf_item)
    cncf_item = prepare_working_space(cncf_item)
    run_gource(cncf_item)