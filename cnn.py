from pymongo import MongoClient
import re
from tqdm import tqdm
from corenlp_pywrap import pywrap

full_annotator_list = ["ssplit"]
cn = pywrap.CoreNLP(url='http://172.17.0.2:9000', annotator_list = full_annotator_list)
c = MongoClient('10.0.1.40')

def clean_speaker(speaker):
    out = re.sub(r'[^a-zA-Z,. ]+', '', speaker)
    return out.strip()
def clean_text(text):
    out = re.sub(r'[\\]+t|[\\]+n', '', text)
    out = re.sub(r"\n|\t|[\\]+|<[/]*p>|\([A-Z ]*\)|\\\\t", '', out)
    out = re.sub(r'\'', "'", out)
    return str(out)

def process_group(group):
    doc = {}
    doc['convo'] = []
    turns = re.split(r'([\n\\n]*[ A-Z,-,{(}{)}]+[:])', group)
    i = 0
    while i < len(turns):
        if re.match(r'([\n\\n]*[ A-Z,-,{(}{)}]+[:])', turns[i]):
            speaker = clean_speaker(turns[i])
            if speaker.upper() == speaker:
                text = clean_text(turns[i+1])
                if text:
                    try:
                        sentences = cn.basic(text).json()['sentences']
                        endOfLast = 0
                        for sent in sentences:
                            last_word = sent['tokens'][-1]
                            sentence_text = text[endOfLast:last_word['characterOffsetEnd']+2]
                            endOfLast = last_word['characterOffsetEnd']+2
                            tokens = list(map(lambda x: x['word'], sent['tokens']))
                            doc['convo'].append({'speaker': speaker, 'text': sentence_text, 'tokens': tokens})
                    except Exception as e:
                        print(text)
            else:
                print(speaker)
                print(i)
            i += 2
        else:
            i += 1
    return doc

def process_doc(raw_doc):
    info = raw_doc['header']
    topics = info.split(';')
    topics[-1] = topics[-1].split("Aired")[0]
    show = raw_doc['show']
    documents = []
    video_splitted = re.split(r'(\(BEGIN [A-Z ]*\)|\(END[A-Z ]*\))', raw_doc['text'])
    video_clips = []
    normal_conv = [""]
    i = 0
    while i < len(video_splitted):
        if re.match(r'\(BEGIN [A-Z ]*\)', video_splitted[i]):
            video_clips.append(video_splitted[i+1])
            normal_conv.append("")
            i += 3
        else:
            normal_conv += video_splitted[i]
            i += 1
    for group in video_clips:
        doc = process_group(group)
        doc['topics'] = topics
        doc['show'] = show
        if len(doc['convo']) > 1:
            documents.append(doc)
    for group in normal_conv:
        doc = process_group(group)
        doc['topics'] = topics
        doc['show'] = show
        if len(doc['convo']) > 1:
            documents.append(doc)
    return documents

if __name__ == '__main__':
    
    documents = []
    for doc in tqdm(c['corpora']['cnn'].find(), total=38000):
        documents.extend(process_doc(doc))
    c['corpora']['cnn_processed'].insert_many(documents)
