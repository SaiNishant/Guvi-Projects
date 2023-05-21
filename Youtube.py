import os
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
import googleapiclient.errors
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import mysql.connector
import numpy as np
from pandas.io import sql
from sqlalchemy import create_engine

# API Key: AIzaSyBAOevBsGQuErUKwIBSLdeX0ryfMS5mBu8

class youtube_harvest():
    
    def __init__(self,api_key,youtube,channel_id):

        self.api_key = api_key
        self.youtube = youtube
        self.channel_id = channel_id
    
    def get_channel_stats(self):

        
        request = self.youtube.channels().list(part = 'snippet,contentDetails,statistics',id = self.channel_id)
        response = request.execute()
        
        channel_info = dict(
                        channel_id = response['items'][0]['id'],
                        channel_name = response['items'][0]['snippet']['title'],
                        channel_description = response['items'][0]['snippet']['description'],
                        Subscribers = response['items'][0]['statistics']['subscriberCount'],
                        View_count = response['items'][0]['statistics']['viewCount'],
                        Total_videos = response['items'][0]['statistics']['videoCount'],
                        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
                        
        

        
        return channel_info
    
    def get_playlist_info(self,playlist_id):
        
        
        request = self.youtube.playlistItems().list(part="snippet, contentDetails",playlistId = playlist_id)
        response = request.execute()
        video_ids = []

        for i in response['items']:
             video_ids.append(i['contentDetails']['videoId'])

        next_page_token  =  response.get('nextPageToken')
        
        while next_page_token is not None:
                request = self.youtube.playlistItems().list(part="snippet, contentDetails",playlistId = playlist_id)
                response = request.execute()
        
        for i in response['items']:
            video_ids.append(i['contentDetails']['videoId'])

        next_page_token  =  response.get('nextPageToken')

        return video_ids  

    def get_video_info(self,video_id):
         
        vid_id = []
        vid_name = [] 
        view_count = []
        like_count = []
        fav_count = []
        vid_duration = []
        vid_des = []
        vid_thumbnail = [] 
        comm_count = []
        chann_id = []
        vid_publish = [] 
 
        for i in range(len(video_id)):
            request = self.youtube.videos().list(part = 'snippet,contentDetails,statistics',id = video_id[i])
            response = request.execute() 
            vid_id.append(video_id[i])
            vid_name.append(response['items'][0]['snippet']['title'])
            view_count.append(response['items'][0]['statistics']['viewCount'])
            like_count.append(response['items'][0]['statistics']['likeCount'])
            fav_count.append(response['items'][0]['statistics']['favoriteCount'])
            comm_count.append(response['items'][0]['statistics']['commentCount'])
            vid_duration.append(response['items'][0]['contentDetails']['duration'])
            vid_des.append(response['items'][0]['snippet']['description'])
            vid_thumbnail.append(response['items'][0]['snippet']['thumbnails']['default']['url'])
            chann_id.append(response['items'][0]['snippet']['channelId'])
            vid_publish.append(response['items'][0]['snippet']['publishedAt'])
                        
        video_dict = dict(video_id = vid_id,
                          video_name = vid_name,
                          video_view_count = view_count,
                          video_like_count = like_count,
                          favourite_count = fav_count,
                          comment_count = comm_count,
                          duration = vid_duration,
                          desc = vid_des,
                          thumbnail = vid_thumbnail,
                          channel_ID = chann_id,
                          video_date = vid_publish
                          )

        
        return video_dict
    
    def get_playlists(self):
         
        request = self.youtube.playlists().list(part = 'snippet,contentDetails',channelId = self.channel_id)
        response = request.execute()  
         
        playlist_id  = []
        chann_id  = []
        playlist_name = []
        for items in response['items']:
           playlist_name.append(items['snippet']['title'])
           playlist_id.append(items['id'])
           chann_id.append(items['snippet']['channelId'])

        play = dict(playlist_name = playlist_name,
                   playlist_id = playlist_id,
                     chann_id = chann_id)
        
        return play

    def get_comment_info(self,video_id):

        vid_id = []
        comment_id = [] 
        comment_author = [] 
        comment_text = [] 
        comment_date = []

        for i in range(len(video_id)):

            try:
                request = self.youtube.commentThreads().list(part = 'snippet',videoId = video_id[i])
                response = request.execute()  
            
                if len(response['items']) > 0:
                
                    for j in response['items']:
            
                        comment_id.append(j['id'])
                        vid_id.append(j['snippet']['videoId'])
                        comment_text.append(j['snippet']['topLevelComment']['snippet']['textDisplay'])
                        comment_author.append(j['snippet']['topLevelComment']['snippet']['authorDisplayName'])
                        comment_date.append(j['snippet']['topLevelComment']['snippet']['publishedAt'])
            
            except:
                
                comment_id.append("")
                vid_id.append(video_id[i])
                comment_text.append("")
                comment_author.append("")
                comment_date.append("")
        
        if len(vid_id)==0:
            vid_id.append("")
            comment_text.append("")
            comment_author.append("")
            comment_date.append("")
            comment_id.append("")
            

        comment = dict(comment_id = comment_id,
                       vid_id = vid_id,
                       comment_text = comment_text,
                       comment_author= comment_author,
                       comment_date= comment_date)        
        return comment

    def send_to_mongodb(self,data_string,collection_name):

        conn = MongoClient()
        dbname = conn.get_database("Youtube")
        collection = dbname[collection_name]

        if collection_name == 'video':
             df = pd.DataFrame(data_string)
             collection.insert_many(df.to_dict('records'))
        if collection_name == 'playlist':
             df = pd.DataFrame(data_string)
             collection.insert_many(df.to_dict('records'))
        if collection_name == 'comment':
            df = pd.DataFrame(data_string)
            collection.insert_many(df.to_dict('records'))
        else:    
             collection.insert_one(data_string)


    def get_all_channel_names(self):

        conn = MongoClient()
        dbname = conn.get_database("Youtube")

        collection = dbname['Final']

        x = collection.find()

        channel_names = []
        for data in x:
            channel_names.append(data['channel_stats']['channel_name'])
        
        #remove duplicates if any
        names = [] 
        for x in channel_names:
             
            if x not in names:

                names.append(x)
        
        return names


class migration():
        
    def get_data_from_Mongo(self,channels):

        conn = MongoClient()
        dbname = conn.get_database("Youtube")

        collection = dbname['Final']

        x = collection.find()

        channel_data = []
        for data in x:
                for i in channels:
                    if data['channel_stats']['channel_name'] == i:
                        channel_data.append(data)
        
        return channel_data

    def get_channel_data(self,channel_data):

        channel_name = [] 
        channel_id = []
        channel_description = []
        Subscribers = [] 
        View_count = [] 
        Total_videos = []
        playlist_id = []
        for i in range(len(channel_data)):
            
            channel_name.append(channel_data[i]['channel_stats']['channel_name'])
            channel_id.append(channel_data[i]['channel_stats']['channel_id'])
            channel_description.append(channel_data[i]['channel_stats']['channel_description'])
            Subscribers.append(int(channel_data[i]['channel_stats']['Subscribers']))
            View_count.append(int(channel_data[i]['channel_stats']['View_count']))
            Total_videos.append(int(channel_data[i]['channel_stats']['Total_videos']))
            playlist_id.append(channel_data[i]['channel_stats']['playlist_id'])
        df = pd.DataFrame(np.column_stack([channel_name,channel_id,channel_description,Subscribers,View_count,Total_videos,
                        playlist_id]),
                         columns= ['channel_name','channel_id','channel_description','Subscribers','View_count',
                                   'Total_videos','playlist_id'] )



        #df = pd.DataFrame(channel_name = channel_name, channel_id = channel_id,channel_description =channel_description,
        #Subscribers = Subscribers, View_count =View_count, Total_videos =Total_videos,playlist_id = playlist_id)
        return df
    
    def get_playlist_data(self,channel_data):

        
        playlist_id = [] 
        playlist_name = []
        channel_id = []
        
        for i in range(len(channel_data)):
            
            for j in range(len(channel_data[i]['playlists']['playlist_name'])):
                playlist_name.append(channel_data[i]['playlists']['playlist_name'][j])
            for k in range(len(channel_data[i]['playlists']['chann_id'])):
                channel_id.append(channel_data[i]['playlists']['chann_id'][k])
            for l in range(len(channel_data[i]['playlists']['playlist_id'])):
                playlist_id.append(channel_data[i]['playlists']['playlist_id'][l])

        df = pd.DataFrame(np.column_stack([playlist_id,playlist_name,channel_id]),
                          columns= ['playlist_id','playlist_name','channel_id']
                          
                          )

        return df

    def get_comment_data(self,channel_data):

        vid_id = []
        comment_id = [] 
        comment_author = [] 
        comment_text = [] 
        comment_date = []
        
        for i in range(len(channel_data)):
            
            for j in range(len(channel_data[i]['comments']['comment_id'])):
                comment_id.append(channel_data[i]['comments']['comment_id'][j])
           
            for k in range(len(channel_data[i]['comments']['vid_id'])):
                vid_id.append(channel_data[i]['comments']['vid_id'][k])

            for l in range(len(channel_data[i]['comments']['comment_author'])):
                comment_author.append(channel_data[i]['comments']['comment_author'][l])

            for m in range(len(channel_data[i]['comments']['comment_text'])):
                comment_text.append(channel_data[i]['comments']['comment_text'][m])

            for n in range(len(channel_data[i]['comments']['comment_date'])):
                comment_date.append(channel_data[i]['comments']['comment_date'][n])

        df = pd.DataFrame(np.column_stack([comment_id,vid_id,comment_author,comment_text,comment_date]),
                        columns= ['comment_id','vid_id','comment_author','comment_text','comment_date']   
                          )

        return df
    
    def convert_duration(self,value):
        
        if value.find('M') == -1:
            return 1
        else:
            return int(value.split('T')[1].split('M')[0])

    def get_video_data(self,channel_data):

        video_id = []
        video_name = [] 
        video_view_count = []
        video_like_count = []
        favorite_count = []
        duration = []
        comment_count = []
        channel_id = []
        video_date = [] 

        for i in range(len(channel_data)):
                
                for j in range(len(channel_data[i]['video_stats']['video_id'])):
                    video_id.append(channel_data[i]['video_stats']['video_id'][j])
                
                for k in range(len(channel_data[i]['video_stats']['video_name'])):
                    video_name.append(channel_data[i]['video_stats']['video_name'][k])
                
                for l in range(len(channel_data[i]['video_stats']['video_view_count'])):
                    video_view_count.append(int(channel_data[i]['video_stats']['video_view_count'][l]))

                for m in range(len(channel_data[i]['video_stats']['video_like_count'])):
                    video_like_count.append(int(channel_data[i]['video_stats']['video_like_count'][m]))
                
                for n in range(len(channel_data[i]['video_stats']['favourite_count'])):
                    favorite_count.append(int(channel_data[i]['video_stats']['favourite_count'][n]))

                for o in range(len(channel_data[i]['video_stats']['comment_count'])):
                    comment_count.append(int(channel_data[i]['video_stats']['comment_count'][o]))
                
                for p in range(len(channel_data[i]['video_stats']['channel_ID'])):
                    channel_id.append(channel_data[i]['video_stats']['channel_ID'][p])
                
                for b in range(len(channel_data[i]['video_stats']['duration'])):
                    value = channel_data[i]['video_stats']['duration'][b]
                    dur = self.convert_duration(value)
                    duration.append(dur)

                for a in range(len(channel_data[i]['video_stats']['video_date'])):
                    value = channel_data[i]['video_stats']['video_date'][a]
                    video_date.append(value.split('T')[0])

        df = pd.DataFrame(np.column_stack([video_id,
        video_name,
        video_view_count,
        video_like_count,
        favorite_count,
        duration,
        comment_count,
        channel_id,
        video_date]),columns=[ 'video_id',
        'video_name',
        'video_view_count',
        'video_like_count',
        'favorite_count',
        'duration',
        'comment_count',
        'channel_id',
        'video_date'])
        return df  

    def migrate_to_sql(self,data,schema):

        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="nishantsai8@P",
        database = "youtube"
        )

        mycursor = mydb.cursor()

        if schema == "Channel":

            for row,col in data.iterrows():
            
                channel_name = col[0] 
                channel_id = col[1]
                channel_description = col[2]
                subscribers = col[3]
                view_count = col[4]
                total_videos = col[5]
                playlist_id = col[6]

                query = """insert into channel (channel_name,channel_id,channel_description,subscribers,view_count,total_videos,playlist_id) 
                    values (%s,%s,%s,%s,%s,%s,%s)"""
            
                record = (channel_name,channel_id,channel_description,subscribers,view_count,total_videos,playlist_id)
                mycursor.execute(query,record)


        if schema == "Playlist":

            for row,col in data.iterrows():

                playlist_id = col[0]
                playlist_name = col[1]
                channel_id = col[2]
                query = """insert into playlist (playlist_id,playlist_name,channel_id) 
                    values (%s,%s,%s)"""
                record = (playlist_id,playlist_name,channel_id)

                mycursor.execute(query,record)

        if schema == "Comment":

            for row,col in data.iterrows():
                
                comment_id = col[0]
                vid_id = col[1]
                comment_author = col[2]
                comment_text = col[3]
                comment_date = col[4]

                query = """insert into comments (comment_id,vid_id,comment_author,comment_text,comment_date) 
                    values (%s,%s,%s,%s,%s)"""

                record = (comment_id,vid_id,comment_author,comment_text,comment_date)

                mycursor.execute(query,record)


        if schema == "Video":

            for row,col in data.iterrows():

                video_id = col[0]
                video_name = col[1]
                video_view_count = col[2]
                video_like_count = col[3]
                favorite_count = col[4]
                duration = col[5]
                comment_count = col[6]
                channel_id = col[7]
                video_date = col[8]

                query = """insert into videos (video_id,
                video_name,
                video_view_count,
                video_like_count,
                favorite_count,
                duration,
                comment_count,
                channel_id,
                video_date) 
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                record = (  video_id,
                            video_name,
                            video_view_count,
                            video_like_count,
                            favorite_count,
                            duration,
                            comment_count,
                            channel_id,
                            video_date)
                
                mycursor.execute(query,record)

        mydb.commit()
        mycursor.close()

class Analysis:

    def query(self,selection):

        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="nishantsai8@P",
        database = "youtube"
        )
        #1
        if selection == "What are the names of all the videos and their corresponding channels?":
            query = """select video_name as "Video Name", channel_name as "Channel Name" from videos 
                    left join channel on videos.channel_id = channel.channel_id"""
        #2
        if selection == "Which channels have the most number of videos, and how many videos do they have?":
            query = """select channel_name as "Channel Name" ,count( distinct video_id) as "Total Videos" from channel
                       left join videos on channel.channel_id = videos.channel_id
                       group by channel_name
                       order by count(distinct video_id) DESC"""
        #3 
        if selection == "What are the top 10 most viewed videos and their respective channels?":
            query = """select video_name as "Video Name",
                    channel_name as "Channel Name",
                    sum(video_view_count) as "Views" from videos
                    left join channel on videos.channel_id = channel.channel_id
                    group by video_name, channel_name
                    order by sum(video_view_count) DESC
                    limit 10"""
        #4    
        if selection == "How many comments were made on each video, and what are their orresponding video names?":
            query = """select video_name as "Video Name",
                    sum(comment_count) as "Total Comments" from videos
                    group by video_name
                    order by sum(comment_count) DESC"""
        #5
        if selection == "Which videos have the highest number of likes, and what are their corresponding channel names?":
            query = """select video_name as "Video Name",
                    channel_name as "Channel Name",
                    sum(video_like_count) as "Likes" from videos
                    left join channel on videos.channel_id = channel.channel_id
                    group by video_name, channel_name
                    order by sum(video_like_count) DESC"""
        #6
        if selection == "What is the total number of likes for each video, and what are their corresponding video names?":
            query = """select video_name as "Video Name",
                    sum(video_like_count) as "Likes" from videos
                    group by video_name"""
        #7
        if selection == "What is the total number of views for each channel, and what are their corresponding channel names?":
            query = """select channel_name as "Channel Name", view_count as "Total Views"
                    from channel"""
        #8
        if selection == "What are the names of all the channels that have published videos in the year 2022?":
        
            query = """select distinct(channel_name) as "Channel Name" from
                (select video_name as "Video Name",channel_name
                from
                videos left join channel on videos.channel_id = channel.channel_id 
                where Year(str_to_date(video_date,'%Y-%m-%d')) = 2022)a"""
            
        #9
        if selection == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            query = """select channel_name as "Channel Name", Truncate(avg(duration),1) as "Average Duration" from 
                    channel left join videos on channel.channel_id = videos.channel_id 
                    group by channel_name
                    order by avg(duration) desc"""
        
        #10
        if selection == "Which videos have the highest number of comments, and what are their corresponding channel names?":
            query = """select channel_name as "Channel Name" , count(distinct comment_id) as "Total Comments" 
                    from channel left join videos on channel.channel_id = videos.channel_id
			        left join comments on videos.video_id = comments.vid_id
                    group by channel_name
                    order by count(distinct comment_id) DESC"""
            

        result = pd.read_sql(query,mydb)

        return result

if __name__ == '__main__':

    st.title("Youtube Data Harvesting Project")
    api_key = "AIzaSyC7ktZA3hGYjmkxWQovcY1y86nhqUv9iRE"
    title = st.text_input("Enter YouTube Channel ID")
    a = st.button("Send Data to Mongo DB")  

#All Channel IDs used in this Project

#1 UC2RqY29NIU7r1uUQwArZlIA
#2 UC-Rgo272A1RekreoF7lZkuQ
#3 UC-EcI50x2OawLjqdouNZPWQ
#4 UC-HTPRCwql-htDEFcEFvWyg
#5 UC--GIC9hgPZeJa6iZqXMZLw
#6 UC-0MCY2erzgB4WqpmEIGgpg
#7 UC-0PCGA_FSGdeyg16QLwEQw
#8 UC-_eVDj3aTFKoAYqSzcybkA
#9 UCjmTxX1jXZPvTwUQHR9tBhg
#10 UCeU5qTuBiqTU5z-hyL6WITQ

    try:
    
        channel_id = title
        #channel_id = "UCo1iK1Y_rAr_CdiCU8hMaqA"
        youtube = build('youtube','v3',developerKey=api_key)
        data = youtube_harvest(api_key,youtube,channel_id)
        channel_stats = data.get_channel_stats()
        #print(channel_stats)
        playlist_id = channel_stats['playlist_id']
        playlist_stats = data.get_playlist_info(playlist_id=playlist_id)
        #print(playlist_stats)
        video_stats = data.get_video_info(video_id=playlist_stats)
        #print(video_stats)
        playlists = data.get_playlists()
        #print(playlists)
        comments = data.get_comment_info(video_id=playlist_stats)

    except:
        st.write("")


    if a:
            final  = dict(channel_stats = channel_stats, 
                          video_stats = video_stats,
                          playlists = playlists,
                          comments = comments)
            st.write(final)
            data.send_to_mongodb(final,"Final")

    all_channels = data.get_all_channel_names()
    selected_channels = st.multiselect("Select Channel(s) to Migrate to SQL:",all_channels)
    b = st.button("Migrate to SQL Database")

    if b:
        migrate = migration()
        mongo_db_info = migrate.get_data_from_Mongo(selected_channels)
        channel = migrate.get_channel_data(mongo_db_info)
        migrate.migrate_to_sql(channel,"Channel")
        playlist = migrate.get_playlist_data(mongo_db_info)
        migrate.migrate_to_sql(playlist,"Playlist")
        comment = migrate.get_comment_data(mongo_db_info)
        migrate.migrate_to_sql(comment,"Comment")
        video = migrate.get_video_data(mongo_db_info)
        migrate.migrate_to_sql(video,"Video")

    queries = ("What are the names of all the videos and their corresponding channels?",
               "Which channels have the most number of videos, and how many videos do they have?",
                "What are the top 10 most viewed videos and their respective channels?",
                "How many comments were made on each video, and what are their orresponding video names?",
                "Which videos have the highest number of likes, and what are their corresponding channel names?",
                "What is the total number of likes for each video, and what are their corresponding video names?",
                "What is the total number of views for each channel, and what are their corresponding channel names?",
                "What are the names of all the channels that have published videos in the year 2022?",
                "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "Which videos have the highest number of comments, and what are their corresponding channel names?"
                )   
    
    query_selected = st.selectbox("Select a Query: ",queries)
    c = st.button("Get Results")
    ana = Analysis()    
    if c:
        result = ana.query(query_selected)
        st.write(result)
