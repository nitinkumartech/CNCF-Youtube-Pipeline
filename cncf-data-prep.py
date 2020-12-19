import yaml
import requests
from pymongo import MongoClient
import git
import os
import shutil

client = MongoClient()
db = client.cncf
cncf_items = db.items

def cleanup_workspace():
    path = os.path.join(os.getcwd(), "pipeline_ws")
    repo_path = os.path.join(path, "repos")
    landscape_path = os.path.join(repo_path, "landscape")
    target_dir = os.path.join(repo_path, os.path.join(path, "logos"))
    if os.path.exists(landscape_path):
        shutil.rmtree(landscape_path)
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    os.mkdir(target_dir)


def update_db():
    r = requests.get('https://raw.githubusercontent.com/cncf/landscape/master/landscape.yml')
    landscape_obj = yaml.load(r.text)
    for category in landscape_obj['landscape']:
        for subcategory in category['subcategories']:
            for item in subcategory['items']:
                if 'repo_url' in item.keys():
                    # Check if name exists
                    item.pop('item', None)
                    mongo_item = cncf_items.find_one({"name": item['name']})
                    if mongo_item == None:
                        item['category'] = category['name']
                        item['subcategory'] = subcategory['name']
                        item['processed'] = False
                        item['uploaded'] = False
                        cncf_items.insert_one(item)

def update_thumbnails():
    path = os.path.join(os.getcwd(), "pipeline_ws")
    repo_path = os.path.join(path, "repos")
    git.Git(repo_path).clone("https://github.com/cncf/landscape.git")
    landscape_path = os.path.join(repo_path, "landscape")
    image_dir_1 = os.path.join(landscape_path, "cached_logos")
    image_dir_2 = os.path.join(landscape_path, "hosted_logos")
    image_dir_3 = os.path.join(landscape_path, "images")

    target_dir = os.path.join(repo_path, os.path.join(path, "logos"))

    file_names = os.listdir(image_dir_1)
    for file_name in file_names:
        if not os.path.exists(os.path.join(target_dir, file_name)):
            shutil.move(os.path.join(image_dir_1, file_name), target_dir)

    file_names = os.listdir(image_dir_2)
    for file_name in file_names:
        if not os.path.exists(os.path.join(target_dir, file_name)):
            shutil.move(os.path.join(image_dir_2, file_name), target_dir)

    file_names = os.listdir(image_dir_3)
    for file_name in file_names:
        if not os.path.exists(os.path.join(target_dir, file_name)):
            shutil.move(os.path.join(image_dir_3, file_name), target_dir)

def delete_source():
    path = os.path.join(os.getcwd(), "pipeline_ws")
    repo_path = os.path.join(path, "repos")
    landscape_path = os.path.join(repo_path, "landscape")
    if os.path.exists(landscape_path):
        shutil.rmtree(landscape_path)

    
cleanup_workspace()  
update_thumbnails()
delete_source()