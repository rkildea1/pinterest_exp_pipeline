CREATE TABLE pinterest_streaming_storage (
category text,
index integer,
unique_id text,
title text,
description text,
follower_count text,
tag_list text,
is_image_or_video text,
image_src text,
downloaded integer,
save_location text
count_of_tags integer,
timeStamp timestamp
);



schema = StructType() \
    .add("category", StringType()) \
    .add("index", IntegerType()) \
    .add("unique_id", StringType()) \
    .add("title", StringType()) \
    .add("description", StringType()) \
    .add("follower_count", StringType()) \
    .add("tag_list", StringType()) \
    .add("is_image_or_video", StringType()) \
    .add("image_src", StringType()) \
    .add("downloaded", IntegerType()) \
    .add("save_location", StringType()) 