from dagster import execute_pipeline, pipeline, solid
import os
from pymongo import MongoClient
import git
import shutil
import subprocess
import requests
import getpass
import hashlib
from time import sleep
import sys
from avatar import grab_avatar
import random

def md5_hex(text):
  m = hashlib.md5()
  m.update(text.encode('ascii', errors='ignore'))
  return m.hexdigest()


def get_data(api_request, username, password):
  r = requests.get(api_request, auth=(username, password), timeout=10)
  data = r.json()
  # Return data
  return data

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
    output_path = os.path.join(video_in_process_path, folder_name)
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    if not os.path.exists(os.path.join(output_path, "avatars")):
        os.mkdir(os.path.join(output_path, "avatars"))
    return mongo_item

@solid
def grab_avatars(context, mongo_item):
    path = os.path.join(os.getcwd(), "pipeline_ws")
    repo_path = os.path.join(path, "repos")
    folder_name = mongo_item['repo_url'].split("/")[-1].split(".")[0]
    project_path = os.path.join(repo_path, folder_name)

    video_in_process_path = os.path.join(path, "video_in_process")
    output_path = os.path.join(video_in_process_path, folder_name)
    avatar_path = os.path.join(output_path, "avatars")
    context.log.info(project_path)
    context.log.info(avatar_path)
    grab_avatar(project_path, avatar_path)
    return mongo_item

@solid
def run_gource(context, mongo_item):
    name = mongo_item['name']
    path = os.path.join(os.getcwd(), "pipeline_ws")
    repo_path = os.path.join(path, "repos")
    video_in_process_path = os.path.join(path, "video_in_process")
    folder_name = mongo_item['repo_url'].split("/")[-1].split(".")[0]
    item_path = os.path.join(repo_path, folder_name)
    output_path = os.path.join(os.path.join(os.path.join(path, video_in_process_path), folder_name), name+".mp4")


    if not mongo_item['logo'].startswith("https"):
        logo_path = os.path.join(os.path.join(path, "processed_logos"), mongo_item['logo'].split(".")[0]+".png")
    else:
        url = mongo_item['logo']
        response = requests.get(url, stream=True)
        if mongo_item['logo'].endswith("svg"):
            d_file = mongo_item['name'] + '.svg'
        else:
            d_file = mongo_item['name'] + '.png'
        with open(os.path.join(os.path.join(path, "logos"), d_file), 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        del response
        if mongo_item['logo'].endswith("svg"):
            os.system("inkscape -z -f " + os.path.join(os.path.join(path, "logos"), d_file) + " -w 250 -j -e " + os.path.join(os.path.join(path, "processed_logos"), d_file.split(".")[0] + ".png"))
        logo_path = os.path.join(os.path.join(path, "processed_logos"), mongo_item['name']+".png")


    avatar_path = os.path.join(os.path.join(os.path.join(os.path.join(path, video_in_process_path), folder_name), "avatars"), "circle_avatars")
    cmd_string = 'gource --hide filenames,dirnames,usernames,progress,mouse --key --screen 2 --frameless -f --title "' + name + ' Development History" --font-size 20 --seconds-per-day 0.1 --auto-skip-seconds 1 -1920x1080 --logo "' + logo_path + '" --path "' + item_path + '" -o - | ffmpeg -y -r 60 -f image2pipe -vcodec ppm -i - -vcodec libx264 -preset ultrafast -pix_fmt yuv420p -crf 1 -threads 0 -bf 0 "' + output_path + '"'
    context.log.info(cmd_string)
    os.system(cmd_string)
    return mongo_item

@solid
def mux_audio(context, mongo_item):
    name = mongo_item['name']
    path = os.path.join(os.getcwd(), "pipeline_ws")
    video_in_process_path = os.path.join(path, "video_in_process")
    folder_name = mongo_item['repo_url'].split("/")[-1].split(".")[0]
    output_path = os.path.join(os.path.join(os.path.join(path, video_in_process_path), folder_name), name+".mp4")
    audio_path = os.path.join(os.getcwd(), "processed_music")
    music_names = os.listdir(audio_path)
    random.shuffle(music_names)
    music = os.path.join(audio_path, music_names[0])
    cmd = 'ffmpeg -i "' + output_path + '" -i "' + music + '" -shortest "' + os.path.join(os.path.join(os.path.join(path, video_in_process_path), folder_name), "processed_"+name+".mp4") +'"'
    context.log.info(cmd)
    os.system(cmd)
    if not os.path.exists(os.path.join(path, "to_upload")):
        os.mkdir(os.path.join(path, "to_upload"))
    shutil.move(os.path.join(os.path.join(os.path.join(path, video_in_process_path), folder_name), "processed_"+name+".mp4"), os.path.join(path, "to_upload"))
    cncf_items.update_one({"_id":mongo_item['_id']},{"$set":{"processed":True}})
    return mongo_item

@solid
def upload_to_youtube(context, mongo_item):
    name = mongo_item['name']
    path = os.path.join(os.getcwd(), "pipeline_ws")
    video_in_process_path = os.path.join(path, "video_in_process")
    folder_name = mongo_item['repo_url'].split("/")[-1].split(".")[0]
    video_path = os.path.join(os.path.join(os.path.join(path, video_in_process_path), folder_name), "processed_"+name+".mp4")
    description = mongo_item['name'] + """ Development Visualization (HD - 1080p)
Suggest new repositories: https://forms.gle/iAkzP1FCzSyEx7Uc7
----

video:
rendered with Gource:
http://code.google.com/p/gource/

music: Epidemic Sound

----

* Native video rendered in 1080p.  Please watch in highest resolution possible to get the extra punch."""
    cmd = 'youtube-upload --title="' + mongo_item['name'] + ' Development History | Gource' + '" --description="'+ description +'" --playlist="CNCF Git Visualizations" --client-secrets="client.json" --category="Science & Technology" --privacy public --embeddable=True ' + video_path
    context.log.info(cmd)
    os.system(cmd)


@solid
def post_pipeline_cleanup(context, mongo_item):
    path = os.path.join(os.getcwd(), "pipeline_ws")
    repo_path = os.path.join(path, "repos")
    folder_name = mongo_item['repo_url'].split("/")[-1].split(".")[0]
    video_in_process_path = os.path.join(path, "video_in_process")
    context.log.info(folder_name)
    shutil.rmtree(os.path.join(repo_path, folder_name))
    shutil.rmtree(os.path.join(video_in_process_path, folder_name))
    os.system("sh launch.sh")
    return mongo_item

@pipeline
def cncf_yt_pipeline():
    cncf_item = get_cncf_item()
    cncf_item = clone_repo(cncf_item)
    cncf_item = prepare_working_space(cncf_item)
    #cncf_item = grab_avatars(cncf_item)
    cncf_item = run_gource(cncf_item)
    cncf_item = mux_audio(cncf_item)
    cncf_item = post_pipeline_cleanup(cncf_item)
    #upload_to_youtube(cncf_item)