#1
select video_name as "Video Name", channel_name as "Channel Name" from videos 
left join channel on videos.channel_id = channel.channel_id;
#2
select channel_name as "Channel Name" ,count( distinct video_id) as "Total Videos" from channel
left join videos on channel.channel_id = videos.channel_id
group by channel_name
order by count(distinct video_id) DESC;
#3
select video_name as "Video Name",
channel_name as "Channel Name",
sum(video_view_count) as "Views" from videos
left join channel on videos.channel_id = channel.channel_id
group by video_name, channel_name
order by sum(video_view_count) DESC
limit 10;
#4
select video_name as "Video Name",
sum(comment_count) as "Total Comments" from videos
group by video_name
order by sum(comment_count) DESC;
#5
select video_name as "Video Name",
channel_name as "Channel Name",
sum(video_like_count) as "Likes" from videos
left join channel on videos.channel_id = channel.channel_id
group by video_name, channel_name
order by sum(video_like_count) DESC;
#6
select video_name as "Video Name",
sum(video_like_count) as "Likes" from videos
group by video_name;

#7
select channel_name as "Channel Name", view_count as "Total Views"
from channel;
#8
select distinct(channel_name) as "Channel Name" from
(select video_name as "Video Name",channel_name
from
videos left join channel on videos.channel_id = channel.channel_id 
where Year(str_to_date(video_date,'%Y-%m-%d')) = 2022)a;
#9
select channel_name as "Channel Name", Truncate(avg(duration),1) as "Average Duration" from 
channel left join videos on channel.channel_id = videos.channel_id 
group by channel_name
order by avg(duration) desc;
#10
select channel_name as "Channel Name" , count(distinct comment_id) as "Total Comments" 
from channel left join videos on channel.channel_id = videos.channel_id
			 left join comments on videos.video_id = comments.vid_id
group by channel_name
order by count(distinct comment_id) DESC;