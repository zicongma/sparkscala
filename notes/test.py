import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import nltk
from tqdm import tqdm

rootdir = ''

for subdir, dirs, files in os.walk(rootdir):
  for file in tqdm(files):
    final_output = ''
    language = None
    file_path=os.path.join(rootdir, file)
    tree=ET.parse(file_path)
    for elem in tree.iter():
      if 'name' in elem.attrib and elem.attrib['name'] == 'Language':
        language = elem.text
      if elem.tag == 'Product':
        if 'language' in elem.attrib:
          language = elem.attrib['language']
      # if elem.tag == 'TextElement' and elem.text != None:
      #   soup = BeautifulSoup(elem.text)
      #   final_output = final_output + soup.get_text() + '\n'
      # if elem.tag == '':
    if language == None:
      print(file)
    # break
