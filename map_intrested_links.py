from sendmail import sendmail
def mil():
    import pandas as pd
    import mysql.connector
    import re
    from datetime import datetime
    mydb = mysql.connector.connect(
        host="localhost",
        user="root"
        )
    mycursor = mydb.cursor()
        
    mycursor.execute("use user_intrest_mails")

    day="is_"+str(datetime.today().strftime('%A')).lower()
    print(day)
    mycursor.execute("use user_intrest_mails")
    mycursor.execute("select* from user_tags where {0} = True".format(day))
    user_tags=mycursor.fetchall()
    user_tags=pd.DataFrame(user_tags)
    user_tags.columns=["name","user","tags","domains","authors","sunday","monday","tuesday","wednesday","thursday","friday","saturday"]
    print(user_tags)
    for i in range(len(user_tags)):
        matching_string=user_tags["tags"][i]+" "+re.sub("'","",user_tags["domains"][i])+" "+user_tags["authors"][i]
        
        mycursor.execute("SELECT question_id qid, question ques, subject sub, description summary, url url, MATCH (question,subject,url_content) AGAINST ('{0}' IN BOOLEAN MODE) score FROM question_test where MATCH (question,subject,url_content) AGAINST ('{0}' IN BOOLEAN MODE) > 0 and exam_name in ({1})ORDER by score DESC limit 5".format(matching_string,user_tags["domains"][i]))
        table = mycursor.fetchall()
        table=pd.DataFrame(table)

        sendmail(user_tags["name"][i],user_tags["user"][i],table)
    
mil()
