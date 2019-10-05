import lxml
import lxml.etree
from datetime import datetime,timedelta
import re
import pandas as pd
from sqlalchemy import create_engine,event
from urllib.parse import quote_plus
import logging
import sys
import numpy as np
import os



# all of the element types in dblp
all_elements = ["article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "mastersthesis", "www"]
# all of the feature types in dblp
all_features = ["address", "author", "booktitle", "cdrom", "chapter", "cite", "crossref", "editor", "ee", "isbn",
                "journal", "month", "note", "number", "pages", "publisher", "school", "series", "title", "url",
                "volume", "year"]



def log_msg(message):
    """Produce a log with current time"""
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message)


def context_iter(dblp_path):
    """Create a dblp data iterator of (event, element) pairs for processing"""
    return lxml.etree.iterparse(source=dblp_path, dtd_validation=True, load_dtd=True)  # required dtd


def clear_element(element):
    """Free up memory for temporary element tree after processing the element"""
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]


def count_pages(pages):
    """Borrowed from: https://github.com/billjh/dblp-iter-parser/blob/master/iter_parser.py
    Parse pages string and count number of pages. There might be multiple pages separated by commas.
    VALID FORMATS:
        51         -> Single number
        23-43      -> Range by two numbers
    NON-DIGITS ARE ALLOWED BUT IGNORED:
        AG83-AG120
        90210H     -> Containing alphabets
        8e:1-8e:4
        11:12-21   -> Containing colons
        P1.35      -> Containing dots
        S2/109     -> Containing slashes
        2-3&4      -> Containing ampersands and more...
    INVALID FORMATS:
        I-XXI      -> Roman numerals are not recognized
        0-         -> Incomplete range
        91A-91A-3  -> More than one dash
        f          -> No digits
    ALGORITHM:
        1) Split the string by comma evaluated each part with (2).
        2) Split the part to subparts by dash. If more than two subparts, evaluate to zero. If have two subparts,
           evaluate by (3). If have one subpart, evaluate by (4).
        3) For both subparts, convert to number by (4). If not successful in either subpart, return zero. Subtract first
           to second, if negative, return zero; else return (second - first + 1) as page count.
        4) Search for number consist of digits. Only take the last one (P17.23 -> 23). Return page count as 1 for (2)
           if find; 0 for (2) if not find. Return the number for (3) if find; -1 for (3) if not find.
    """
    cnt = 0
    for part in re.compile(r",").split(pages):
        subparts = re.compile(r"-").split(part)
        if len(subparts) > 2:
            continue
        else:
            try:
                re_digits = re.compile(r"[\d]+")
                subparts = [int(re_digits.findall(sub)[-1]) for sub in subparts]
            except IndexError:
                continue
            cnt += 1 if len(subparts) == 1 else subparts[1] - subparts[0] + 1
    return "" if cnt == 0 else str(cnt)


def extract_feature(elem, features, include_key=True):
    """Extract the value of each feature"""
    if include_key:
        attribs = {'key': [elem.attrib['key']]}
    else:
        attribs = {}
    for feature in features:
        attribs[feature] = []
    for sub in elem:
        if sub.tag not in features:
            continue
        if sub.tag == 'title':
            text = re.sub("<.*?>", "", lxml.etree.tostring(sub).decode('utf-8')) if sub.text is None else sub.text
        elif sub.tag == 'pages':
            text = count_pages(sub.text)
        else:
            text = sub.text
        if text is not None and len(text) > 0:
            attribs[sub.tag] = attribs.get(sub.tag) + [text]
    return attribs



def parse_entity(dblp_path,  type_name, features=None,  include_key=False):
    """Parse specific elements according to the given type name and features"""
    log_msg("PROCESS: Start parsing for {}...".format(str(type_name)))
    assert features is not None, "features must be assigned before parsing the dblp dataset"
    results = []
    attrib_count, full_entity, part_entity = {}, 0, 0
    counter=0
    for _, elem in context_iter(dblp_path):
        if elem.tag in type_name:
            attrib_values = extract_feature(elem, features, include_key)  # extract required features
            results.append(attrib_values)  # add record to results array
            for key, value in attrib_values.items():
                attrib_count[key] = attrib_count.get(key, 0) + len(value)
            cnt = sum([1 if len(x) > 0 else 0 for x in list(attrib_values.values())])
            if cnt == len(features):
                full_entity += 1
            else:
                part_entity += 1
        elif elem.tag not in all_elements:
            continue
        clear_element(elem)
        counter=counter+1
        if(counter>17):
            break
    
    df=pd.DataFrame(results,columns=['key']+features)
    return df

def write_to_db(df, database_name, table_name):
    """
    Creates a sqlalchemy engine and write the dataframe to database
    """
    # replacing infinity by nan
    #df = df.replace([np.inf, -np.inf], np.nan)
    chunk_size = 5000

    #DRIVER={MySQL ODBC 3.51 Driver}; SERVER=localhost; PORT=3306;DATABASE=nameDBase; UID=root; PASSWORD=12345;
    #conn='Driver={MySQL ODBC 3.51 Driver};Server=.\localhost;Database=dblp_scheme;Trusted_Connection=yes;'
    #conn =  "DRIVER={SQL     Server};SERVER="+db_addr+";DATABASE="+database_name+";UID="+user_name+";PWD="+pwd+""
    #quoted = quote_plus(conn)
    #new_con = 'mysql+pyodbc:///?odbc_connect={}'.format(quoted)pip install mysql-connector==2.1.4

#https://creativedata.atlassian.net/wiki/spaces/SAP/pages/61177950/Python+-+Read+Write+tables+from+MySQL+with+Security

   # engine = create_engine(
   #     'mysql+mysqlconnector://' + os.environ['MYSQL_USER'] + ':' + os.environ['MYSQL_PASSWORD'] + '@' + os.environ[
   #         'MYSQL_HOST_IP'] + ':' + os.environ['MYSQL_PORT'] + '/sandbox', echo=False)

    # create sqlalchemy engine
    engine = create_engine('mysql+mysqlconnector://root@localhost:3306/dblp_scheme')

  
    # WARNING!! -- overwrites the table using if_exists='replace'
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=chunk_size)

def main():
    dblp_path ='C:/Users/Ali/PycharmProjects/Assignment1/dblp.xml'

    try:
        context_iter(dblp_path)
        log_msg("LOG: Successfully loaded \"{}\".".format(dblp_path))
    except IOError:
        log_msg("ERROR: Failed to load file \"{}\". Please check your XML and DTD files.".format(dblp_path))
        exit()

    df_article=parse_entity(dblp_path,['article'], features=['title','pages','url','journal','month','volume','publisher','year','booktitle','crossref','editor','cite','number','note','ee','cdrom'], include_key=True)
    for col in df_article.columns:
        df_article[col]=df_article[col].str.get(0)
    print('inside main')
    print(str(df_article))
    
    write_to_db(df_article,'dblp_scheme','articles')

    df_book=parse_entity(dblp_path,['book'], features= ['title', 'pages', 'url', 'volume','publisher','year','booktitle','series','editor','ee'])
    for col in df_book.columns:
     df_book[col] = df_book[col].str.get(0)
    print('inside main')
    print(str(df_book))

    write_to_db(df_book, 'dblp_scheme', 'books')

    log_msg("FINISHED...")

def main():
 dblp_path ='C:/Users/Ali/PycharmProjects/Assignment1/dblp.xml'

try:
    context_iter(dblp_path)
    log_msg("LOG: Successfully loaded \"{}\".".format(dblp_path))
except IOError:
    log_msg("ERROR: Failed to load file \"{}\". Please check your XML and DTD files.".format(dblp_path))
    exit()
    

df_article=parse_entity(dblp_path,['article'], features=['title','pages','url','author','journal','month','volume','publisher','year','booktitle','crossref','editor','cite','number','note','ee','cdrom'], include_key=True)
for col in df_article.columns:
    df_article[col]=df_article[col].str.get(0)
print('inside main')
print(str(df_article))
write_to_db(df_article, 'dblp_scheme', 'articles')

df_book = parse_entity(dblp_path, ['book'],features=['title', 'pages', 'url', 'volume', 'publisher', 'year', 'booktitle', 'series',
                                 'editor', 'ee'])
for col in df_book.columns:
    df_book[col] = df_book[col].str.get(0)
print('inside main')
print(str(df_book))
write_to_db(df_book, 'dblp_scheme', 'books')


if __name__ == '__main__':
   main()



