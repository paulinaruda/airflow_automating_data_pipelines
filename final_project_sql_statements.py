
class SqlQueries:
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2' 
        JSON '{}'
    """
    
    songplay_table_create = ("""
    CREATE TABLE songplays (
    sp_session_id bigint not null,
    sp_item_in_session bigint not null,
    sp_start_time TIMESTAMP,
    sp_user_id varchar(20) not null,
    sp_level VARCHAR(5),
    sp_song_id varchar(20),
    sp_artist_id varchar(20),
    sp_location VARCHAR(50),
    sp_user_agent VARCHAR(255),
    PRIMARY KEY (sp_session_id, sp_item_in_session)
    );
    """)      
    
    songplay_table_insert = ("""
        INSERT INTO songplays (sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, sp_item_in_session, sp_location, sp_user_agent)
        SELECT DISTINCT
            TIMESTAMP 'epoch' + INTERVAL '1 second' * (l.ts / 1000)::bigint AS sp_start_time,
            CAST(l.userId AS integer) AS sp_user_id,
            l.level as sp_level,
            s.song_id as sp_song_id,
            s.artist_id as sp_artist_id,
            l.sessionid AS sp_session_id,
            l.iteminsession  as sp_item_in_session,
            l.location as sp_location,
            l.userAgent AS sp_user_agent
        FROM staging_events l
        LEFT JOIN staging_songs s ON (l.song = s.title AND l.artist = s.artist_name and l.length = s.duration)
        WHERE l.page = 'NextSong';
    """)

    user_table_insert = ("""
        insert into users (user_id, first_name, last_name, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        and userid != ' '
    """)

    song_table_insert = ("""
        insert into song (song_id, title, artists_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        insert into artist (artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name as name, artist_location as location, 
        artist_latitude as latitude, artist_longitude as longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT distinct start_time, hour, day, week, month, year, weekday from
    (SELECT
    sp_start_time AS start_time,
    EXTRACT(hour FROM sp_start_time) AS hour,
    EXTRACT(day FROM sp_start_time) AS day,
    EXTRACT(week FROM sp_start_time) AS week,
    EXTRACT(month FROM sp_start_time) AS month,
    EXTRACT(year FROM sp_start_time) AS year,
    EXTRACT(dow FROM sp_start_time) AS weekday
    FROM songplays)
    ;
    """)
    
    staging_logs_table_create= ("""
        create table if not exists staging_events
        (
        artist        varchar(255),
        auth          varchar(20),
        firstName     varchar(20),
        gender        varchar(4),
        itemInSession bigint,      
        lastName      varchar(10), 
        length        float, 
        level         varchar(5)  not null,
        location      varchar(50), 
        method        varchar(5)  not null, 
        page          varchar(30) not null, 
        registration  float, 
        sessionId     bigint    not null, 
        song          varchar(255), 
        status        bigint    not null, 
        ts            bigint    not null, 
        userAgent     varchar(255),
        userId        varchar(20) not null 
        );
        """)
    
    staging_songs_table_create = ("""
        create table if not exists staging_songs
        (
        num_songs         integer     not null, 
        artist_id         varchar(20),
        artist_latitude   float,
        artist_longitude  float,
        artist_location   varchar(255),
        artist_name       varchar(255), 
        song_id           varchar(20),
        title             varchar(255) not null,  
        duration          float,
        year              integer
        );
        """)
    
    
    user_table_create = ("""
    create table if not exists users 
    (
    user_id           varchar(20),
    first_name        varchar(20),
    last_name         varchar(20), 
    gender            varchar(4),
    level             varchar(5)   not null
    );
    """)


    song_table_create = ("""
    create table if not exists song
    (
    song_id          varchar(20), 
    title            varchar(255) not null,
    artists_id       varchar(20), 
    year             integer,
    duration         float
    );
    """)


    artist_table_create = ("""
    create table if not exists artist
    (
    artist_id        varchar(20),
    name             varchar(255),
    location         varchar(255),
    latitude         float,
    longitude        float
    );
    """)

    time_table_create = ("""
    create table if not exists time
    (
    start_time       timestamp not null,
    hour             integer not null,
    day              integer not null,
    week             integer not null,
    month            integer not null,
    year             integer not null,
    weekday          integer not null
    );
    """)

########################

    append_songplays_insert = ("""
    INSERT INTO songplays (sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, sp_item_in_session, sp_location, sp_user_agent)
    SELECT DISTINCT
    TIMESTAMP 'epoch' + INTERVAL '1 second' * (l.ts / 1000)::bigint AS sp_start_time,
    CAST(l.userId AS integer) AS sp_user_id,
    l.level as sp_level,
    s.song_id as sp_song_id,
    s.artist_id as sp_artist_id,
    l.sessionid AS sp_session_id,
    l.iteminsession as sp_item_in_session,
    l.location as sp_location,
    l.userAgent AS sp_user_agent
    FROM staging_events l
    LEFT JOIN staging_songs s ON (l.song = s.title AND l.artist = s.artist_name and l.length = s.duration)
    WHERE l.page = 'NextSong'
    AND NOT EXISTS (
    SELECT 1
    FROM songplays
    WHERE sp_session_id = l.sessionid AND sp_item_in_session = l.iteminsession
    );
    """)
    
    append_time_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time, hour, day, week, month, year, weekday
    FROM (
    SELECT
        sp_start_time AS start_time,
        EXTRACT(hour FROM sp_start_time) AS hour,
        EXTRACT(day FROM sp_start_time) AS day,
        EXTRACT(week FROM sp_start_time) AS week,
        EXTRACT(month FROM sp_start_time) AS month,
        EXTRACT(year FROM sp_start_time) AS year,
        EXTRACT(dow FROM sp_start_time) AS weekday
    FROM songplays
    ) AS subquery
    WHERE NOT EXISTS (
    SELECT 1
    FROM time
    WHERE time.start_time = subquery.start_time
    AND time.hour = subquery.hour
    AND time.day = subquery.day
    AND time.week = subquery.week
    AND time.month = subquery.month
    AND time.year = subquery.year
    AND time.weekday = subquery.weekday
    );
    """)
    
    append_user_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userid, firstname, lastname, gender, level
    FROM staging_events AS se
    WHERE page = 'NextSong'
    AND userid != ' '
    AND NOT EXISTS (
    SELECT 1
    FROM users AS u
    WHERE u.user_id = se.userid
    );
    """)
    
    append_song_insert = ("""
    INSERT INTO song (song_id, title, artists_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs AS s
    WHERE NOT EXISTS (
    SELECT 1
    FROM song AS t
    WHERE t.song_id = s.song_id
    AND t.title = s.title
    AND t.artists_id = s.artist_id
    AND t.year = s.year
    AND t.duration = s.duration
    );                    
    """)
    
    append_artist_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, 
    artist_latitude AS latitude, artist_longitude AS longitude
    FROM staging_songs AS ss
    WHERE NOT EXISTS (
    SELECT 1
    FROM artist AS a
    WHERE a.artist_id = ss.artist_id
    );
    """)
    