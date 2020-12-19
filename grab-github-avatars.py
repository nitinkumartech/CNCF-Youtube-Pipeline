# -*- coding: utf-8 -*-
#
# Based on:
# https://code.google.com/p/gource/wiki/GravatarExample
# https://gist.github.com/macagua/5c2f5e4e38df92aae7fe
# 
# Usage with Gource: gource --user-image-dir .git/avatar/
# 
# Get list of authors + email with git log
# git log --format='%aN|%aE' | sort -u
#
# Get list of authors + email with hg log (todo)
# hg log --template 'author: {author}\n'
#

import requests
import getpass
import os
import subprocess
import hashlib
from time import sleep
import sys

def md5_hex(text):
  m = hashlib.md5()
  m.update(text.encode('ascii', errors='ignore'))
  return m.hexdigest()


def get_data(api_request, username, password):
  r = requests.get(api_request, auth=(username, password), timeout=10)
  data = r.json()
  # Return data
  return data

if __name__ == "__main__":

  # Clear screen
  os.system('cls' if os.name == 'nt' else 'clear')

  # Login to the GitHub API
  username = ""
  password = ""

  # Configure the path of the git project
  projectpath = "/Users/nitin/yt-automation/pipeline_ws/repos/treasuremap"
  gitpath = os.path.join(projectpath, '.git')
  output_dir = os.path.join(projectpath, 'avatar')

  # Create the folder for storing the images. It's in the .git folder, so it won't be tracked by git
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)

  # Get the authors from the git log
  gitlog = subprocess.check_output(
      ['git', 'log', '--pretty=format:%ae|%an'], cwd=projectpath)
  authors = set(gitlog.decode('ascii', errors='ignore').splitlines())
  print("")
  print("USERS:")
  print(authors)

  # Check each author
  for author in authors:
    # Get e-mail and name from log
    email, name = author.split('|')
    print("")
    print("Checking", name, email)
    # Try to find the user on GitHub with the e-mail
    api_request = "https://api.github.com/search/users?utf8=%E2%9C%93&q=" + \
        email + "+in%3Aemail&type=Users"
    data = get_data(api_request, username, password)

    # Check if the user was found
    if "items" in data.keys():
      if len(data["items"]) == 1:
        url = data["items"][0]["avatar_url"]
        print("Avatar url:", url)
      else:
        # Try to find the user on GitHub with the name
        api_request = "https://api.github.com/search/users?utf8=%E2%9C%93&q=" + \
            name + "+in%3Aname&type=Users"
        data = get_data(api_request, username, password)

        # Check if the user was found
        if "items" in data.keys():
          if len(data["items"]) == 1:
            url = data["items"][0]["avatar_url"]
            print("Avatar url:", url)
          # Eventually try to find the user with Gravatar
          else:
            url = "http://www.gravatar.com/avatar/" + \
                md5_hex(email) + "?d=identicon&s=" + str(90)
            print("Avatar url:", url)

    # Finally retrieve the image
    try:
      output_file = os.path.join(output_dir, name + '.png')
      if not os.path.exists(output_file):
        r = requests.get(url)
        if r.ok:
          with open(output_file, 'wb') as img:
            img.write(r.content)
    except:
      print("There was an error with", name, email)