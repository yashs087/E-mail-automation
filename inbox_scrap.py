'''
####################### Creator #######################

Created by Yogesh Dixit

Created on 11th August 2020

#######################################################
'''

import schedule
import gspread
import time

from oauth2client.service_account import ServiceAccountCredentials
def job():
    
    

    
    
    ####################### imports and libraries ###########################
    import pytz
    import json
    import requests
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import imaplib 
    import email
    from tqdm import tqdm
    import pandas as pd
    from bs4 import BeautifulSoup
    import re
    import sys
    from datetime import datetime, timedelta, time
    import datetime as dt
    from urllib.parse import unquote
    from datetime import datetime
    
    ################################# gsheet to verify trusted mails #####################################
    print ("access gsheet")
    
    gsheeturl="" #link to url of excel sheet containing the name and email for verification
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('file_name.json', scope) #the json file contains the login credentials
    client = gspread.authorize(creds)
    sheet = client.open_by_url(gsheeturl).sheet1
    check = sheet.col_values(2)
    name = sheet.col_values(3)
    print (name)
    
    
    
    ############################# Scrap Inbox #########################################################
    
    
    import pytz
    
    def convert_datetime_timezone(dt, tz1, tz2):
        tz1 = pytz.timezone(tz1)
        tz2 = pytz.timezone(tz2)
    
        dt = datetime.strptime(dt,"%Y-%m-%d %H:%M:%S")
        dt = tz1.localize(dt)
        dt = dt.astimezone(tz2)
        dt = dt.strftime("%Y-%m-%d %H:%M:%S")
    
        return dt
    
    
    
    
    
    
    date_format = "%d-%b-%Y" # DD-Mon-YYYY 
    since_date = datetime.strptime(datetime.strftime(datetime.now() - timedelta(7) + timedelta(hours=5,minutes=30),'%d-%b-%Y'), date_format) #the dates need to be changed accordingly
    before_date = datetime.strptime((datetime.now()+ timedelta(hours=1,minutes=00)).strftime('%d-%b-%Y'), date_format)
    print(datetime.now(),since_date,before_date)
    try:
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login('email', 'pass')
        mail.select('inbox')
    except:
        print('login error')
    '''type, data = mail.search(None,
    '(since "%s" before "%s")' % (since_date.strftime(date_format),
                                     before_date.strftime(date_format)))'''
                                     
                                
    type, data = mail.search(None,
    '(since "%s")' % (since_date.strftime(date_format)))                            
                                     
    print(datetime.now(),since_date,before_date)
    mail_ids = data[0]
    
    id_list = mail_ids.split() 
    id_list.reverse()
    email_from=[]
    text=[]
    links=[]
    #for count in range(0,len(id_list)):
    flag=False
    count=0
    while count<len(id_list) and flag == False:
        print("count",count)
        typ, data = mail.fetch(id_list[count], '(RFC822)' )
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_string(response_part[1].decode('utf-8','ignore'))
                
                ########################## setting time frame #####################################
                try:
                    
                    time1="".join(msg["date"].split("+")[0].split("-")[0].strip().split(","))
                    timelap=int(msg["date"].split(" ")[5])
                    hour=int(timelap/100)
                    minutes=int(timelap%100)
                    print(timelap)
                    recieving_time=datetime.strptime(time1,"%a %d %b %Y %H:%M:%S")+ timedelta(hours=5,minutes=30)- timedelta(hours=hour,minutes=minutes)
                    india = pytz.timezone('Asia/Kolkata')
                    datetime_india = datetime.now(india)
                    time0=datetime.strptime(datetime_india.strftime("%d-%b-%Y %H:%M:%S"),"%d-%b-%Y %H:%M:%S")
                    print(msg["from"])
                    print("UST time",msg['date'])
                    print("india time:",time0 )
                    print("mail recieved at",recieving_time)
                    print("time diffence",time0-recieving_time)
                    if timedelta(days=0, hours=1,minutes=0)<(time0-recieving_time):
                        print("bayond given time frame ie ",time0-recieving_time)
                        flag=True
                        print(count)
                except:
                    continue
                ################################ scraping mails from time frame #######################
                
                email_subject = msg['subject']
                email_from.append(msg['from'])
                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        body = part.get_payload(decode=True)
                        body=body.decode('utf-8','ignore')
                        link=re.findall(r'(https?://\S+)', body)
                        links.append(link)
                        text.append(BeautifulSoup(body, "lxml").text)
        count=count+1
        #print(email_from)
    #print("email_from")
    print ("completed reading data");
    
    
    ####################### Extracting senter's Name & Email #################################
    
    names=[]
    emails=[]
    #split email_from into names and emails
    for i in range(0,len(email_from)):
        lists=email_from[i].split('<')
        names.append(lists[0])
        try:
            emails.append(lists[1])
        except:
            emails.append('')
    for i in range(0,len(emails)):
        emails[i]=emails[i].replace('>','')
    
    print("emails extracted")
    ################################ Filtering Links ###########################################

    
    def filter(url):
        #print("in filter")
        head= url.split('?')[0]
        #print(head,sep,tail)
        splitted=head.split('/')
        #print(splitted)
        greatest=0
        for splits in splitted:
            count1=splits.count('-')
            if count1 > greatest:
                greatest = count1
        count2=head.count('_')
        if greatest > 2 or count2 > 2:
            #print(item)
            return True,head
        else:
            return False,head
        
    
    #print("hello baby")
    hlinks=[]
    denied=['images','image','jpg','png','jpeg','img','open','track_click','privacy','terms','gif','unsubscribe','quora','help','support','xhtml','settings','</a><b' ,'policy']
    for ind,i in enumerate(links):
        temp=[]
        print(emails[ind],"loop1")
        
        
        #to verify the mail
        if emails[ind] not in check or names in name:
            print(ind,"Not trusted mail")
            continue
        
        
        
        print("trusted mail from",emails[ind] )
        
        i=set(i)
        for item in i:
            item=item.split('"')[0].split("'")[0]
            print("loop2, varified mail")
            #print(item)
            #[print(ele) in item for ele in denied]
            
            if any(ele in item for ele in denied) == False:
                try:
                    text=requests.get("https://epo-scrapping.herokuapp.com/scrape",params={"url":item}).text
                    text= json.loads(text)
                except:
                    print("skipped due to error")
                    continue
                
                
                vailid,link=filter(text["decoded_url"])
                
                if vailid!=True:
                    continue
                print("original url",item)
                print("decoded url",text["decoded_url"])
                print("vailidated and filtered url",vailid,link)
                #print(text["scraped"]["cleaned_text"])
                
                
                
                ########################## generate tags ################################
                
                def clean_text(text_val):
                    text_val = str(text_val)
                    text_val = text_val.replace("nan", "")
                    text_val = text_val.replace('"', '')
                    text_val = text_val.replace("'", "")
                    return ''.join([i if ord(i) < 128 else ' ' for i in text_val])
                
                
                
                all_tags = pd.read_excel('tag_file.xlsx')
                all_tags.dropna(inplace=True,subset=['TagName'])
                all_tags['TagName'] = [str(x).lower().strip() for x in all_tags['TagName']]
                #data.dropna(inplace=True,subset=['title'])
                all_tags.drop_duplicates(inplace=True)
                all_tags.reset_index(drop=True,inplace=True)
                #all_tags=list(all_tags)
                #print(all_tags.TagName.values)
                #print(clean_text(text["scraped"]["cleaned_text"]))
                
                from flashtext import KeywordProcessor
                keyword_processor = KeywordProcessor()
                keyword_processor.add_keywords_from_list(list(all_tags.TagName.values))
                tags1=keyword_processor.extract_keywords(clean_text(text["scraped"]["cleaned_text"]))
                
                '''
                for i in text['scraped']:
                    print(i)
                    if i!="html":
                        try:
                            print(text['scraped'][i])
                        except:
                            print(text['scraped'][i].encode("utf-8"))
                            
                '''
                
                ######################## Connecting Database ############################
    


                mydb = mysql.connector.connect(
                    host="localhost",
                    user="root"
                    )
                mycursor = mydb.cursor()
        
                mycursor.execute("use user_intrest_mails")
                
                ####################### insert into database #########################################
                try:   
                    if len(clean_text(text["scraped"]['Title']))==0:
                        continue
                    title="'"+clean_text(text["scraped"]['Title'])+"'"
                    link="'"+clean_text(link)+"'"
                    domain=link.split(".")
                    #dlist=[".com",".in",".org",".net",".edu",".gov"]
                    print(link)
                    print(link.split("/"))
                    for i in link.split("/"):
                        if ".com" in i or ".in" in i or ".org" in i or ".net" in i or ".edu" in i or ".gov" in i:
                            domain=i
                            print(i)
                    #domain="'"+domain+"'"
                    if len(domain)>0:
                        subject="'"+clean_text(','.join(text["scraped"]['authors']))+" - "+domain+"'"
                    else:
                        subject="'"+clean_text(','.join(text["scraped"]['authors']))+"'"
                    description="'"+clean_text(text["scraped"]["meta_description"])+"'"
                    tags=text["scraped"]['tags']
                    tags=tags+tags1
                    tags="'"+clean_text(','.join(tags))+"'"
                    #print(link)
                    query="""INSERT INTO `question_test`(`question`, `subject`, `exam_name`, `url_content`, `description`, `url`) VALUES ({0},{1},{2},{3},{4},{5})""".format(title,subject,"'"+domain+"'",tags,description,link)
                    #query="""INSERT INTO `scraped_links`(`link`,  `tags`) VALUES ({0},{1})""".format(link,tags)
                    #print(query)
                    
                    
                    mycursor.execute(query)
                    print("hello")
                    mydb.commit()
                    mydb.close()
                    
                except:
                    pass
                


job()
'''
schedule.every(59).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1) '''
