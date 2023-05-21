create table channel(
channel_name varchar(100),
channel_id varchar(100) primary key,
channel_description varchar(10000),
Subscribers int,
View_count int,
Total_videos int,
playlist_id varchar(100)
);

create table playlist(
playlist_id varchar(100) primary key,
playlist_name varchar(1000),
channel_id varchar(1000)
);

create table comments(
comment_id varchar(100),
vid_id varchar(100),
comment_author varchar(100),
comment_text varchar(1000),
comment_date varchar(100)
);

create table videos(

video_id varchar(100),
video_name varchar(100),
video_view_count int,
video_like_count int,
favorite_count int,
comment_count int, 
duration int,
channel_id varchar(100),
video_date varchar(100)
)


