-- keyspace:table
CREATE TABLE pinterest_ks.data 

( 
    unique_id text PRIMARY KEY, 
    category text, 
    count_of_tags int, 
    description text, 
    downloaded int, 
    follower_count text, 
    image_src text, 
    is_image_or_video text, 
    save_location text, 
    source_index int, 
    tag_list text, 
    title text 
    
);
