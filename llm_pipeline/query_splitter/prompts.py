system_base = """
You are an expert SQL query splitter across engines. 
Your task is to carefully study the SQL queries, and based on their structure, and operations you will split the query into two queries that will run sequentially on two different engines (DuckDB and Datafusion).
IMPORTANT: Your primary goal is to split the query in such a way that the overall performance of the distributed execution across the two engines will lead to a better performance in terms of execution time. 
"""

# Stack Overflow schema
system_schema_so = """
Here is the stackoverflow_dba schema:
create table PostHistoryTypes (
    Id   smallint    not null,
    Name varchar(50) not null,
    primary key (Id)
);
create table LinkTypes (
    Id   smallint    not null,
    Name varchar(50) not null,
    primary key (Id)
);
create table PostTypes (
    Id   smallint    not null,
    Name varchar(50) not null,
    primary key (Id)
);
create table CloseReasonTypes (
    Id   smallint    not null,
    Name varchar(50) not null,
    primary key (Id)
);
create table VoteTypes (
    Id   smallint    not null,
    Name varchar(50) not null,
    primary key (Id)
);

create table Users (
    Id              int       not null primary key,
    Reputation      int       not null,
    CreationDate    timestamp not null,
    DisplayName     varchar(40),
    LastAccessDate  timestamp not null, /* The time when the user last loaded a page; updated every 30 min at most */
    WebsiteUrl      varchar(200),
    Location        varchar(300),
    AboutMe         text,
    Views           int, /* Number of times the profile is viewed */
    UpVotes         int, /* How many upvotes the user has cast */
    DownVotes       int, /* How many downvotes the user has cast */
    ProfileImageUrl varchar(200),
    AccountId       int /* User's Stack Exchange Network profile ID */
);

create table Badges (
    Id       int         not null primary key,
    UserId   int         not null,
    Name     varchar(50) not null, /* Name of the badge */
    Date     timestamp   not null, /* Date the badge was awarded, e.g. 2008-09-15T08:55:03.923 */
    Class    smallint    not null, /* 1 (Gold), 2 (Silver), or 3 (Bronze) */
    TagBased bool        not null /* True if badge is for a tag, otherwise it is a named badge */
);

create table Posts (
    Id                    int not null primary key,
    PostTypeId            smallint references PostTypes (Id), /* (listed in the PostTypes table)
    - 1 = Question
    - 2 = Answer
    - 3 = Wiki
    - 4 = TagWikiExcerpt
    - 5 = TagWiki
    - 6 = ModeratorNomination
    - 7 = WikiPlaceholder (Appears to include auxiliary site content like the help center introduction, election description, and the tour page's introduction, ask, and don't ask sections)
    - 8 = PrivilegeWiki*/
    AcceptedAnswerId      int, /* only present if PostTypeId = 1 */
    ParentId              int, /* only present if PostTypeId = 2 */
    CreationDate          timestamp,
    Score                 int, /* generally non-zero for only Questions, Answers, and Moderator Nominations */
    ViewCount             int,
    Body                  text, /* as rendered HTML, not Markdown */
    OwnerUserId           int references Users (Id), /* only present if user has not been deleted; always -1 for tag wiki entries, i.e. the community user owns them */
    OwnerDisplayName      varchar(40),
    LastEditorUserId      int references Users (Id),
    LastEditorDisplayName varchar(40),
    LastEditDate          timestamp, /* e.g. 2009-03-05T22:28:34.823 - the date and time of the most recent edit to the post */
    LastActivityDate      timestamp, /* e.g. 2009-03-11T12:51:01.480 - datetime of the post's most recent activity */
    Title                 varchar(300), /* question title (PostTypeId = 1), or on Stack Overflow, the tag name for some tag wikis and excerpts (PostTypeId = 4/5) */
    Tags                  varchar(4000), /* question tags (PostTypeId = 1), or on Stack Overflow, the subject tag of some tag wikis and excerpts (PostTypeId = 4/5) */
    AnswerCount           int, /* the number of undeleted answers (only present if PostTypeId = 1) */
    CommentCount          int,
    FavoriteCount         int,
    ClosedDate            timestamp, /* present only if the post is closed */
    CommunityOwnedDate    timestamp, /* present only if post is community wiki'd */
    ContentLicense        varchar(30)
);

create table Comments (
    Id              int           not null primary key,
    PostId          int           not null references Posts (Id),
    Score           int,
    Text            varchar(2000) not null, /* Comment body */
    CreationDate    timestamp     not null,
    UserDisplayName varchar(40),
    UserId          int references Users (Id), /* optional, absent if user has been deleted */
    ContentLicense  varchar(30)
);

create table PostHistory (
    Id                int not null primary key,
    PostHistoryTypeId smallint references PostHistoryTypes (Id), /* (listed in the PostHistoryTypes table)
    - 1 = Initial Title (initial title (questions only))
    - 2 = Initial Body (initial post raw body text)
    - 3 = Initial Tags (initial list of tags (questions only)
    - 4 = Edit Title (modified title (questions only))
    - 5 = Edit Body (modified post body (raw markdown))
    - 6 = Edit Tags (modified list of tags (questions only))
    - 7 = Rollback Title (reverted title (questions only))
    - 8 = Rollback Body (reverted body (raw markdown))
    - 9 = Rollback Tags (reverted list of tags (questions only))
    - 10 = Post Closed (post voted to be closed)
    - 11 = Post Reopened (post voted to be reopened)
    - 12 = Post Deleted (post voted to be removed)
    - 13 = Post Undeleted (post voted to be restored)
    - 14 = Post Locked (post locked by moderator)
    - 15 = Post Unlocked (post unlocked by moderator)
    - 16 = Community Owned (post now community owned)
    - 17 = Post Migrated (post migrated - now replaced by 35/36 (away/here))
    - 18 = Question Merged (question merged with deleted question)
    - 19 = Question Protected (question was protected by a moderator)
    - 20 = Question Unprotected (question was unprotected by a moderator)
    - 22 = Question Unmerged (answers/votes restored to previously merged question)
    - 24 = Suggested Edit Applied
    - 25 = Post Tweeted
    - 31 = Discussion moved to chat
    - 33 = Post Notice Added (comment contains foreign key to PostNotices)
    - 34 = Post Notice Removed (comment contains foreign key to PostNotices)
    - 35 = Post Migrated Away (replaces id 17)
    - 36 = Post Migrated Here (replaces id 17)
    - 37 = Post Merge Source
    - 38 = Post Merge Destination
    - 50 = CommunityBump (bumped by community user)
    - 52 = SelectedHotQuestion (question became hot network question)
    - 53 = RemovedHotQuestion (question removed from hot network)
    - 66 = CreatedFromWizard */
    PostId            int references Posts (Id),
    RevisionGUID      varchar(36), /* At times more than one type of history record can be recorded by a single action. All of these will be grouped using the same RevisionGUID */
    CreationDate      timestamp,
    UserId            int references Users (Id),
    UserDisplayName   varchar(40), /* populated if a user has been removed and no longer referenced by user id */
    Comment           varchar(800), /* This field will contain the comment made by the user who edited a post
    - If PostHistoryTypeId = 10, this field contains the CloseReasonId of the close reason (listed in CloseReasonTypes):
        - Old close reasons:
            - 1 = Exact Duplicate
            - 2 = Off-topic
            - 3 = Subjective and argumentative
            - 4 = Not a real question
            - 7 = Too localized
            - 10 = General reference
            - 20 = Noise or pointless (Meta sites only)
        - Current close reasons:
            - 101 = Duplicate
            - 102 = Off-topic
            - 103 = Needs details or clarity
            - 104 = Needs more focus
            - 105 = Opinion-based
    - If PostHistoryTypeId in (33,34) this field contains the PostNoticeId of the PostNotice */
    Text              text, /*  A raw version of the new value for a given revision
    - If PostHistoryTypeId in (10,11,12,13,14,15,19,20,35) this column will contain a JSON encoded string with all users who have voted for the PostHistoryTypeId
    - If it is a duplicate close vote, the JSON string will contain an array of original questions as OriginalQuestionIds
    - If PostHistoryTypeId = 17 this column will contain migration details of either from <url> or to <url> */
    ContentLicense    varchar(30)
);

create table PostLinks (
    Id            bigint    not null primary key,
    CreationDate  timestamp not null, /* when the link was created */
    PostId        int       not null references Posts (Id), /* id of source post */
    RelatedPostId int       not null references Posts (Id), /* id of target/related post */
    LinkTypeId    smallint  not null references LinkTypes (Id) /* (listed in the LinkTypes table)
    - 1 = Linked (PostId contains a link to RelatedPostId)
    - 3 = Duplicate (PostId is a duplicate of RelatedPostId) */
);

create table Tags (
    Id              int not null primary key,
    TagName         varchar(35),
    Count           int not null,
    ExcerptPostId   int references Posts (Id), /* Id of Post that holds the excerpt text of the tag */
    WikiPostId      int references Posts (Id), /* Id of Post that holds the wiki text of the tag */
    IsModeratorOnly bool,
    IsRequired      bool
);

create table Votes (
    Id           int      not null primary key,
    PostId       int      not null, /* references Posts (Id), (not enforced in the data) */
    VoteTypeId   smallint not null references VoteTypes (Id), /*  (listed in the VoteTypes table)
    - 1 = AcceptedByOriginator
    - 2 = UpMod (AKA upvote)
    - 3 = DownMod (AKA downvote)
    - 4 = Offensive
    - 5 = Favorite (AKA bookmark; UserId will also be populated) feature removed after October 2022 / replaced by Saves
    - 6 = Close (effective 2013-06-25: Close votes are only stored in table: PostHistory)
    - 7 = Reopen
    - 8 = BountyStart (UserId and BountyAmount will also be populated)
    - 9 = BountyClose (BountyAmount will also be populated)
    - 10 = Deletion
    - 11 = Undeletion
    - 12 = Spam
    - 14 = NominateModerator
    - 15 = ModeratorReview (i.e., a moderator looking at a flagged post)
    - 16 = ApproveEditSuggestion */
    UserId       int references Users (Id),
    CreationDate timestamp,
    BountyAmount int
);

"""

# JOB schema
system_schema_job = """
Here is the IMDb (Join Order Benchmark / JOB) schema.

CREATE TABLE aka_name (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL,
    name character varying,
    imdb_index character varying(3),
    name_pcode_cf character varying(11),
    name_pcode_nf character varying(11),
    surname_pcode character varying(11),
    md5sum character varying(65)
);

CREATE TABLE aka_title (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    title character varying,
    imdb_index character varying(4),
    kind_id integer NOT NULL,
    production_year integer,
    phonetic_code character varying(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    note character varying(72),
    md5sum character varying(32)
);

CREATE TABLE cast_info (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL,
    movie_id integer NOT NULL,
    person_role_id integer,
    note character varying,
    nr_order integer,
    role_id integer NOT NULL
);

CREATE TABLE char_name (
    id integer NOT NULL PRIMARY KEY,
    name character varying NOT NULL,
    imdb_index character varying(2),
    imdb_id integer,
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);

CREATE TABLE comp_cast_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(32) NOT NULL
);

CREATE TABLE company_name (
    id integer NOT NULL PRIMARY KEY,
    name character varying NOT NULL,
    country_code character varying(6),
    imdb_id integer,
    name_pcode_nf character varying(5),
    name_pcode_sf character varying(5),
    md5sum character varying(32)
);

CREATE TABLE company_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(32)
);

CREATE TABLE complete_cast (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer,
    subject_id integer NOT NULL,
    status_id integer NOT NULL
);

CREATE TABLE info_type (
    id integer NOT NULL PRIMARY KEY,
    info character varying(32) NOT NULL
);

CREATE TABLE keyword (
    id integer NOT NULL PRIMARY KEY,
    keyword character varying NOT NULL,
    phonetic_code character varying(5)
);

CREATE TABLE kind_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(15)
);

CREATE TABLE link_type (
    id integer NOT NULL PRIMARY KEY,
    link character varying(32) NOT NULL
);

CREATE TABLE movie_companies (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    company_id integer NOT NULL,
    company_type_id integer NOT NULL,
    note character varying
);

CREATE TABLE movie_info_idx (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info character varying NOT NULL,
    note character varying(1)
);

CREATE TABLE movie_keyword (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    keyword_id integer NOT NULL
);

CREATE TABLE movie_link (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    linked_movie_id integer NOT NULL,
    link_type_id integer NOT NULL
);

CREATE TABLE name (
    id integer NOT NULL PRIMARY KEY,
    name character varying NOT NULL,
    imdb_index character varying(9),
    imdb_id integer,
    gender character varying(1),
    name_pcode_cf character varying(5),
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);

CREATE TABLE role_type (
    id integer NOT NULL PRIMARY KEY,
    role character varying(32) NOT NULL
);

CREATE TABLE title (
    id integer NOT NULL PRIMARY KEY,
    title character varying NOT NULL,
    imdb_index character varying(5),
    kind_id integer NOT NULL,
    production_year integer,
    imdb_id integer,
    phonetic_code character varying(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    series_years character varying(49),
    md5sum character varying(32)
);

CREATE TABLE movie_info (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info character varying NOT NULL,
    note character varying
);

CREATE TABLE person_info (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info character varying NOT NULL,
    note character varying
);


Key relationships (commonly used; not enforced as FK constraints in the raw JOB schema):
- aka_name.person_id        -> name.id
- aka_title.movie_id        -> title.id
- aka_title.kind_id         -> kind_type.id
- aka_title.episode_of_id   -> title.id         (parent series for an episode; nullable)

- cast_info.person_id       -> name.id
- cast_info.movie_id        -> title.id
- cast_info.role_id         -> role_type.id
- cast_info.person_role_id  -> char_name.id     (character played; nullable)

- movie_companies.movie_id      -> title.id
- movie_companies.company_id    -> company_name.id
- movie_companies.company_type_id -> company_type.id

- movie_info.movie_id       -> title.id
- movie_info.info_type_id   -> info_type.id

- movie_info_idx.movie_id     -> title.id
- movie_info_idx.info_type_id -> info_type.id

- movie_keyword.movie_id    -> title.id
- movie_keyword.keyword_id  -> keyword.id

- movie_link.movie_id       -> title.id
- movie_link.linked_movie_id -> title.id
- movie_link.link_type_id   -> link_type.id

- person_info.person_id     -> name.id
- person_info.info_type_id  -> info_type.id

- complete_cast.movie_id    -> title.id
- complete_cast.subject_id  -> comp_cast_type.id
"""

# TPCH Schema
system_schema_tpch = """
Here is the TPC-H (Transaction Processing Performance Council) schema.

CREATE TABLE part (
    p_partkey     BIGINT NOT NULL PRIMARY KEY,
    p_name        VARCHAR NOT NULL,
    p_mfgr        VARCHAR NOT NULL,
    p_brand       VARCHAR NOT NULL,
    p_type        VARCHAR NOT NULL,
    p_size        INTEGER NOT NULL,
    p_container   VARCHAR NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment     VARCHAR NOT NULL
);

CREATE TABLE supplier (
    s_suppkey     BIGINT NOT NULL PRIMARY KEY,
    s_name        VARCHAR NOT NULL,
    s_address     VARCHAR NOT NULL,
    s_nationkey   INTEGER NOT NULL,
    s_phone       VARCHAR NOT NULL,
    s_acctbal     DECIMAL(15,2) NOT NULL,
    s_comment     VARCHAR NOT NULL
);

CREATE TABLE partsupp (
    ps_partkey    BIGINT NOT NULL,
    ps_suppkey    BIGINT NOT NULL,
    ps_availqty   BIGINT NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment    VARCHAR NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE customer (
    c_custkey     BIGINT NOT NULL PRIMARY KEY,
    c_name        VARCHAR NOT NULL,
    c_address     VARCHAR NOT NULL,
    c_nationkey   INTEGER NOT NULL,
    c_phone       VARCHAR NOT NULL,
    c_acctbal     DECIMAL(15,2) NOT NULL,
    c_mktsegment  VARCHAR NOT NULL,
    c_comment     VARCHAR NOT NULL
);

CREATE TABLE orders (
    o_orderkey      BIGINT NOT NULL PRIMARY KEY,
    o_custkey       BIGINT NOT NULL,
    o_orderstatus   VARCHAR NOT NULL,
    o_totalprice    DECIMAL(15,2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority VARCHAR NOT NULL,
    o_clerk         VARCHAR NOT NULL,
    o_shippriority  INTEGER NOT NULL,
    o_comment       VARCHAR NOT NULL
);

CREATE TABLE lineitem (
    l_orderkey      BIGINT NOT NULL,
    l_partkey       BIGINT NOT NULL,
    l_suppkey       BIGINT NOT NULL,
    l_linenumber    BIGINT NOT NULL,
    l_quantity      DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount      DECIMAL(15,2) NOT NULL,
    l_tax           DECIMAL(15,2) NOT NULL,
    l_returnflag    VARCHAR NOT NULL,
    l_linestatus    VARCHAR NOT NULL,
    l_shipdate      DATE NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  VARCHAR NOT NULL,
    l_shipmode      VARCHAR NOT NULL,
    l_comment       VARCHAR NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
);

CREATE TABLE nation (
    n_nationkey   INTEGER NOT NULL PRIMARY KEY,
    n_name        VARCHAR NOT NULL,
    n_regionkey   INTEGER NOT NULL,
    n_comment     VARCHAR NOT NULL
);

CREATE TABLE region (
    r_regionkey   INTEGER NOT NULL PRIMARY KEY,
    r_name        VARCHAR NOT NULL,
    r_comment     VARCHAR NOT NULL
);

Key relationships:
- lineitem.l_orderkey -> orders.o_orderkey
- lineitem.l_partkey  -> part.p_partkey
- lineitem.l_suppkey  -> supplier.s_suppkey
- lineitem.(l_partkey, l_suppkey) -> partsupp.(ps_partkey, ps_suppkey)

- orders.o_custkey    -> customer.c_custkey

- partsupp.ps_partkey -> part.p_partkey
- partsupp.ps_suppkey -> supplier.s_suppkey

- customer.c_nationkey -> nation.n_nationkey
- supplier.s_nationkey -> nation.n_nationkey

- nation.n_regionkey   -> region.r_regionkey
"""








# Stack overflow 1GB
system_tables_info_so_1 = """
=== Table: Badges ===
Row count: 439352
  Column: Id | type=INTEGER | distinct=439352 | nulls=0 | min=1 | max=482789
  Column: UserId | type=INTEGER | distinct=154017 | nulls=0 | min=2 | max=308122
  Column: Name | type=VARCHAR | distinct=258 | nulls=0
  Column: Date | type=TIMESTAMP | distinct=319439 | nulls=0 | min=2011-01-03 20:19:04.957000 | max=2024-09-30 23:41:10.127000
  Column: Class | type=SMALLINT | distinct=3 | nulls=0 | min=1 | max=3
  Column: TagBased | type=BOOLEAN | distinct=1 | nulls=0

=== Table: CloseReasonTypes ===
Row count: 12
  Column: Id | type=SMALLINT | distinct=12 | nulls=0 | min=1 | max=105
  Column: Name | type=VARCHAR | distinct=12 | nulls=0

=== Table: Comments ===
Row count: 351440
  Column: Id | type=INTEGER | distinct=351440 | nulls=0 | min=1 | max=666039
  Column: PostId | type=INTEGER | distinct=122542 | nulls=0 | min=1 | max=342701
  Column: Score | type=INTEGER | distinct=44 | nulls=0 | min=0 | max=89
  Column: Text | type=VARCHAR | distinct=349258 | nulls=0
  Column: CreationDate | type=TIMESTAMP | distinct=351428 | nulls=0 | min=2008-09-16 19:03:26.467000 | max=2024-09-30 23:38:34.447000
  Column: UserDisplayName | type=VARCHAR | distinct=3231 | nulls=337396
  Column: UserId | type=INTEGER | distinct=41821 | nulls=14045 | min=-1 | max=307961
  Column: ContentLicense | type=VARCHAR | distinct=3 | nulls=0

=== Table: LinkTypes ===
Row count: 2
  Column: Id | type=SMALLINT | distinct=2 | nulls=0 | min=1 | max=3
  Column: Name | type=VARCHAR | distinct=2 | nulls=0

=== Table: PostHistory ===
Row count: 847593
  Column: Id | type=INTEGER | distinct=847593 | nulls=0 | min=1 | max=1254471
  Column: PostHistoryTypeId | type=SMALLINT | distinct=30 | nulls=0 | min=1 | max=66
  Column: PostId | type=INTEGER | distinct=246673 | nulls=0 | min=1 | max=342708
  Column: RevisionGUID | type=VARCHAR | distinct=591220 | nulls=0
  Column: CreationDate | type=TIMESTAMP | distinct=564506 | nulls=0 | min=2008-09-16 19:00:31.183000 | max=2024-09-30 23:52:11.427000
  Column: UserId | type=INTEGER | distinct=65628 | nulls=66262 | min=-1 | max=308003
  Column: UserDisplayName | type=VARCHAR | distinct=6507 | nulls=817098
  Column: Comment | type=VARCHAR | distinct=108544 | nulls=530823
  Column: Text | type=VARCHAR | distinct=632224 | nulls=122249
  Column: ContentLicense | type=VARCHAR | distinct=3 | nulls=121987

=== Table: PostHistoryTypes ===
Row count: 34
  Column: Id | type=SMALLINT | distinct=34 | nulls=0 | min=1 | max=66
  Column: Name | type=VARCHAR | distinct=34 | nulls=0

=== Table: PostLinks ===
Row count: 20146
  Column: Id | type=BIGINT | distinct=20146 | nulls=0 | min=125 | max=8493380
  Column: CreationDate | type=TIMESTAMP | distinct=16493 | nulls=0 | min=2011-01-03 23:15:52.947000 | max=2024-09-30 11:37:43.113000
  Column: PostId | type=INTEGER | distinct=13237 | nulls=0 | min=2 | max=342693
  Column: RelatedPostId | type=INTEGER | distinct=9440 | nulls=0 | min=1 | max=342606
  Column: LinkTypeId | type=SMALLINT | distinct=2 | nulls=0 | min=1 | max=3

=== Table: PostTypes ===
Row count: 8
  Column: Id | type=SMALLINT | distinct=8 | nulls=0 | min=1 | max=8
  Column: Name | type=VARCHAR | distinct=8 | nulls=0

=== Table: Posts ===
Row count: 246673
  Column: Id | type=INTEGER | distinct=246673 | nulls=0 | min=1 | max=342708
  Column: PostTypeId | type=SMALLINT | distinct=6 | nulls=0 | min=1 | max=7
  Column: AcceptedAnswerId | type=INTEGER | distinct=50405 | nulls=196268 | min=4 | max=342708
  Column: ParentId | type=INTEGER | distinct=92043 | nulls=106179 | min=1 | max=342706
  Column: CreationDate | type=TIMESTAMP | distinct=245268 | nulls=0 | min=2008-09-16 19:00:31.183000 | max=2024-09-30 22:49:43.850000
  Column: Score | type=INTEGER | distinct=238 | nulls=0 | min=-12 | max=2043
  Column: ViewCount | type=INTEGER | distinct=16745 | nulls=142236 | min=6 | max=3260864
  Column: Body | type=VARCHAR | distinct=246294 | nulls=0
  Column: OwnerUserId | type=INTEGER | distinct=65122 | nulls=6651 | min=-1 | max=308003
  Column: OwnerDisplayName | type=VARCHAR | distinct=8206 | nulls=232377
  Column: LastEditorUserId | type=INTEGER | distinct=18577 | nulls=131880 | min=-1 | max=307961
  Column: LastEditorDisplayName | type=VARCHAR | distinct=175 | nulls=242963
  Column: LastEditDate | type=TIMESTAMP | distinct=104442 | nulls=128271 | min=2011-01-03 21:23:51.230000 | max=2024-10-02 19:59:50.047000
  Column: LastActivityDate | type=TIMESTAMP | distinct=186534 | nulls=0 | min=2008-09-16 19:10:01.690000 | max=2024-10-03 02:09:45.613000
  Column: Title | type=VARCHAR | distinct=104345 | nulls=142236
  Column: Tags | type=VARCHAR | distinct=46843 | nulls=142236
  Column: AnswerCount | type=INTEGER | distinct=21 | nulls=142236 | min=0 | max=22
  Column: CommentCount | type=INTEGER | distinct=31 | nulls=0 | min=0 | max=37
  Column: FavoriteCount | type=INTEGER | distinct=0 | nulls=246673 | min=<NA> | max=<NA>
  Column: ClosedDate | type=TIMESTAMP | distinct=7056 | nulls=239617 | min=2011-01-05 04:00:42.367000 | max=2024-10-02 06:26:25.703000
  Column: CommunityOwnedDate | type=TIMESTAMP | distinct=955 | nulls=245675 | min=2011-01-05 04:06:31.143000 | max=2024-09-26 13:03:42.113000
  Column: ContentLicense | type=VARCHAR | distinct=3 | nulls=0

=== Table: Tags ===
Row count: 1232
  Column: Id | type=INTEGER | distinct=1232 | nulls=0 | min=1 | max=2702
  Column: TagName | type=VARCHAR | distinct=1232 | nulls=0
  Column: Count | type=INTEGER | distinct=357 | nulls=0 | min=1 | max=34586
  Column: ExcerptPostId | type=INTEGER | distinct=734 | nulls=498 | min=1237 | max=340306
  Column: WikiPostId | type=INTEGER | distinct=734 | nulls=498 | min=1236 | max=340305
  Column: IsModeratorOnly | type=BOOLEAN | distinct=0 | nulls=1232
  Column: IsRequired | type=BOOLEAN | distinct=0 | nulls=1232

=== Table: Users ===
Row count: 267193
  Column: Id | type=INTEGER | distinct=267193 | nulls=0 | min=-1 | max=308122
  Column: Reputation | type=INTEGER | distinct=1601 | nulls=0 | min=1 | max=183985
  Column: CreationDate | type=TIMESTAMP | distinct=267192 | nulls=0 | min=2011-01-03 17:13:13.357000 | max=2024-09-30 23:22:32.080000
  Column: DisplayName | type=VARCHAR | distinct=232154 | nulls=0
  Column: LastAccessDate | type=TIMESTAMP | distinct=267192 | nulls=0 | min=2011-01-03 17:13:13.357000 | max=2024-10-03 02:47:07.263000
  Column: WebsiteUrl | type=VARCHAR | distinct=52131 | nulls=209674
  Column: Location | type=VARCHAR | distinct=22016 | nulls=150978
  Column: AboutMe | type=VARCHAR | distinct=77324 | nulls=181775
  Column: Views | type=INTEGER | distinct=501 | nulls=0 | min=0 | max=44224
  Column: UpVotes | type=INTEGER | distinct=443 | nulls=0 | min=0 | max=20703
  Column: DownVotes | type=INTEGER | distinct=141 | nulls=0 | min=0 | max=11209
  Column: ProfileImageUrl | type=VARCHAR | distinct=0 | nulls=267193
  Column: AccountId | type=INTEGER | distinct=267192 | nulls=0 | min=-1 | max=36091954

=== Table: VoteTypes ===
Row count: 15
  Column: Id | type=SMALLINT | distinct=15 | nulls=0 | min=1 | max=16
  Column: Name | type=VARCHAR | distinct=15 | nulls=0

=== Table: Votes ===
Row count: 926084
  Column: Id | type=INTEGER | distinct=926084 | nulls=0 | min=1 | max=1081403
  Column: PostId | type=INTEGER | distinct=242635 | nulls=0 | min=1 | max=342704
  Column: VoteTypeId | type=SMALLINT | distinct=14 | nulls=0 | min=1 | max=16
  Column: UserId | type=INTEGER | distinct=929 | nulls=924604 | min=-1 | max=290611
  Column: CreationDate | type=TIMESTAMP | distinct=5406 | nulls=0 | min=2008-08-04 00:00:00 | max=2024-09-30 00:00:00
  Column: BountyAmount | type=INTEGER | distinct=12 | nulls=923671 | min=25 | max=500

=== Table: subplan ===
Row count: 101563
  Column: displayname | type=VARCHAR | distinct=45981 | nulls=0
  Column: title | type=VARCHAR | distinct=101484 | nulls=0
  Column: creationdate | type=TIMESTAMP | distinct=101563 | nulls=0 | min=2008-09-24 01:40:48.050000 | max=2024-09-30 22:49:25.743000
  Column: PostId | type=INTEGER | distinct=101563 | nulls=0 | min=1 | max=342707
"""








# Stack overflow 10GB
system_tables_info_so_10 = """
=== Table: badges ===
Row count: 2321724
  Column: id | type=INTEGER | distinct=2321724 | nulls=0 | min=1 | max=2502707
  Column: userid | type=INTEGER | distinct=549021 | nulls=0 | min=2 | max=1416532
  Column: name | type=VARCHAR | distinct=726 | nulls=0
  Column: date | type=TIMESTAMP | distinct=1070380 | nulls=0 | min=2010-07-20 19:07:22.990000 | max=2024-09-30 23:56:55.167000
  Column: class | type=SMALLINT | distinct=3 | nulls=0 | min=1 | max=3
  Column: tagbased | type=BOOLEAN | distinct=1 | nulls=0

=== Table: closereasontypes ===
Row count: 12
  Column: id | type=SMALLINT | distinct=12 | nulls=0 | min=1 | max=105
  Column: name | type=VARCHAR | distinct=12 | nulls=0

=== Table: comments ===
Row count: 7241822
  Column: id | type=INTEGER | distinct=7241822 | nulls=0 | min=3 | max=10663063
  Column: postid | type=INTEGER | distinct=2163672 | nulls=0 | min=1 | max=4978525
  Column: score | type=INTEGER | distinct=181 | nulls=0 | min=0 | max=841
  Column: text | type=VARCHAR | distinct=7099985 | nulls=0
  Column: creationdate | type=TIMESTAMP | distinct=7241473 | nulls=0 | min=2010-03-27 15:55:07.370000 | max=2024-09-30 23:58:40.830000
  Column: userdisplayname | type=VARCHAR | distinct=11373 | nulls=6929633
  Column: userid | type=INTEGER | distinct=253091 | nulls=311081 | min=-1 | max=1416480
  Column: contentlicense | type=VARCHAR | distinct=3 | nulls=0

=== Table: linktypes ===
Row count: 2
  Column: id | type=SMALLINT | distinct=2 | nulls=0 | min=1 | max=3
  Column: name | type=VARCHAR | distinct=2 | nulls=0

=== Table: posthistory ===
Row count: 11795244
  Column: id | type=INTEGER | distinct=11795244 | nulls=0 | min=1 | max=17382683
  Column: posthistorytypeid | type=SMALLINT | distinct=31 | nulls=0 | min=1 | max=66
  Column: postid | type=INTEGER | distinct=3850116 | nulls=0 | min=1 | max=4978528
  Column: revisionguid | type=VARCHAR | distinct=7786303 | nulls=0
  Column: creationdate | type=TIMESTAMP | distinct=7631990 | nulls=0 | min=2010-03-27 14:33:20.727000 | max=2024-09-30 23:55:27.903000
  Column: userid | type=INTEGER | distinct=374116 | nulls=820507 | min=-1 | max=1416539
  Column: userdisplayname | type=VARCHAR | distinct=15559 | nulls=11261522
  Column: comment | type=VARCHAR | distinct=728143 | nulls=7398122
  Column: text | type=VARCHAR | distinct=9552094 | nulls=479133
  Column: contentlicense | type=VARCHAR | distinct=3 | nulls=519141

=== Table: posthistorytypes ===
Row count: 34
  Column: id | type=SMALLINT | distinct=34 | nulls=0 | min=1 | max=66
  Column: name | type=VARCHAR | distinct=34 | nulls=0

=== Table: postlinks ===
Row count: 412991
  Column: id | type=BIGINT | distinct=412991 | nulls=0 | min=13 | max=102778746
  Column: creationdate | type=TIMESTAMP | distinct=369350 | nulls=0 | min=2010-07-21 05:09:26.900000 | max=2024-09-30 23:36:50.840000
  Column: postid | type=INTEGER | distinct=270058 | nulls=0 | min=5 | max=4978518
  Column: relatedpostid | type=INTEGER | distinct=184245 | nulls=0 | min=1 | max=4978451
  Column: linktypeid | type=SMALLINT | distinct=2 | nulls=0 | min=1 | max=3

=== Table: posts ===
Row count: 3850116
  Column: id | type=INTEGER | distinct=3850116 | nulls=0 | min=1 | max=4978528
  Column: posttypeid | type=SMALLINT | distinct=7 | nulls=0 | min=1 | max=7
  Column: acceptedanswerid | type=INTEGER | distinct=870559 | nulls=2979557 | min=7 | max=4978527
  Column: parentid | type=INTEGER | distinct=1354571 | nulls=1672823 | min=1 | max=4978504
  Column: creationdate | type=TIMESTAMP | distinct=3845569 | nulls=0 | min=2010-03-27 14:33:20.727000 | max=2024-09-30 23:55:24.180000
  Column: score | type=INTEGER | distinct=431 | nulls=0 | min=-72 | max=1637
  Column: viewcount | type=INTEGER | distinct=24616 | nulls=2181952 | min=2 | max=726632
  Column: body | type=VARCHAR | distinct=3848845 | nulls=0
  Column: owneruserid | type=INTEGER | distinct=371631 | nulls=164700 | min=-1 | max=1416539
  Column: ownerdisplayname | type=VARCHAR | distinct=18629 | nulls=3678469
  Column: lasteditoruserid | type=INTEGER | distinct=148067 | nulls=2121877 | min=-1 | max=1416471
  Column: lasteditordisplayname | type=VARCHAR | distinct=6272 | nulls=3762647
  Column: lasteditdate | type=TIMESTAMP | distinct=1722003 | nulls=2041227 | min=2010-07-20 19:26:37.047000 | max=2024-10-03 02:47:26.313000
  Column: lastactivitydate | type=TIMESTAMP | distinct=2856361 | nulls=0 | min=2010-07-20 19:14:10.603000 | max=2024-10-03 03:08:00.590000
  Column: title | type=VARCHAR | distinct=1666562 | nulls=2181952
  Column: tags | type=VARCHAR | distinct=403475 | nulls=2181952
  Column: answercount | type=INTEGER | distinct=51 | nulls=2181952 | min=0 | max=164
  Column: commentcount | type=INTEGER | distinct=75 | nulls=0 | min=0 | max=111
  Column: favoritecount | type=INTEGER | distinct=0 | nulls=3850116 | min=<NA> | max=<NA>
  Column: closeddate | type=TIMESTAMP | distinct=111530 | nulls=3738586 | min=2010-07-31 17:35:23.297000 | max=2024-10-03 00:33:40.513000
  Column: communityowneddate | type=TIMESTAMP | distinct=17830 | nulls=3829811 | min=2010-07-20 19:20:00.543000 | max=2024-09-30 00:04:43.257000
  Column: contentlicense | type=VARCHAR | distinct=3 | nulls=0

=== Table: posttypes ===
Row count: 8
  Column: id | type=SMALLINT | distinct=8 | nulls=0 | min=1 | max=8
  Column: name | type=VARCHAR | distinct=8 | nulls=0

=== Table: tags ===
Row count: 1976
  Column: id | type=INTEGER | distinct=1976 | nulls=0 | min=1 | max=7945
  Column: tagname | type=VARCHAR | distinct=1976 | nulls=0
  Column: count | type=INTEGER | distinct=1024 | nulls=0 | min=2 | max=147746
  Column: excerptpostid | type=INTEGER | distinct=1892 | nulls=84 | min=4433 | max=4975718
  Column: wikipostid | type=INTEGER | distinct=1892 | nulls=84 | min=4432 | max=4975717
  Column: ismoderatoronly | type=BOOLEAN | distinct=0 | nulls=1976
  Column: isrequired | type=BOOLEAN | distinct=0 | nulls=1976

=== Table: users ===
Row count: 1260710
  Column: id | type=INTEGER | distinct=1260710 | nulls=0 | min=-1 | max=1416570
  Column: reputation | type=INTEGER | distinct=6092 | nulls=0 | min=1 | max=622546
  Column: creationdate | type=TIMESTAMP | distinct=1260701 | nulls=0 | min=2010-07-20 14:51:15.677000 | max=2024-09-30 23:57:26.150000
  Column: displayname | type=VARCHAR | distinct=1045736 | nulls=0
  Column: lastaccessdate | type=TIMESTAMP | distinct=1260672 | nulls=0 | min=2010-07-20 14:51:15.677000 | max=2024-10-03 03:56:09.293000
  Column: websiteurl | type=VARCHAR | distinct=65192 | nulls=1139704
  Column: location | type=VARCHAR | distinct=31466 | nulls=1079323
  Column: aboutme | type=VARCHAR | distinct=128998 | nulls=1083960
  Column: views | type=INTEGER | distinct=2960 | nulls=0 | min=0 | max=241407
  Column: upvotes | type=INTEGER | distinct=2113 | nulls=0 | min=0 | max=101999
  Column: downvotes | type=INTEGER | distinct=643 | nulls=0 | min=0 | max=64977
  Column: profileimageurl | type=VARCHAR | distinct=0 | nulls=1260710
  Column: accountid | type=INTEGER | distinct=1260694 | nulls=0 | min=-1 | max=36092684

=== Table: votes ===
Row count: 12744093
  Column: id | type=INTEGER | distinct=12744093 | nulls=0 | min=1 | max=15873012
  Column: postid | type=INTEGER | distinct=3965227 | nulls=0 | min=1 | max=4978525
  Column: votetypeid | type=SMALLINT | distinct=14 | nulls=0 | min=1 | max=16
  Column: userid | type=INTEGER | distinct=12142 | nulls=12703806 | min=-1 | max=1400285
  Column: creationdate | type=TIMESTAMP | distinct=5189 | nulls=0 | min=2010-03-27 00:00:00 | max=2024-09-30 00:00:00
  Column: bountyamount | type=INTEGER | distinct=16 | nulls=12681376 | min=0 | max=500

=== Table: votetypes ===
Row count: 15
  Column: id | type=SMALLINT | distinct=15 | nulls=0 | min=1 | max=16
  Column: name | type=VARCHAR | distinct=15 | nulls=0
"""










system_output = """ 
CRITICAL OUTPUT REQUIREMENTS:

- You may decide to NOT cut the query if you believe cutting will not improve performance.

- If you decide NOT to cut, you MUST output EXACTLY this JSON (and nothing else):
  {"has_cut": false}

- If you decide to cut, you MUST output EXACTLY one JSON object (and nothing else) with this schema:
  {
    "has_cut": true,
    "q1_engine": "duckdb" | "datafusion",
    "q2_engine": "duckdb" | "datafusion",
    "sql1": "<complete SQL statement>",
    "sql2": "<complete SQL statement>"
  }

- The output MUST be a single, valid JSON object. No extra text, no markdown fences.
- There are ONLY two subqueries when has_cut=true (sql1 and sql2).
- The second query can reference the output relation from the first subquery as: s1
- There is only 1 output table that can come out from the first query and that can be referenced in the second.
- DataFusion does not yet support IN (SELECT …) correlated subqueries in the JOIN condition.
- SELECT INTO is not supported!
"""



def _load_resource_text(filename: str) -> str:
    from pathlib import Path
    base = Path(__file__).resolve().parent.parent / "resources"
    path = base / filename
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


SYSTEM_SCHEMA_BY_NAME = {
    "SO": system_schema_so,
    "JOB": system_schema_job,
    "TPCH1": system_schema_tpch,
    "TPCH10": system_schema_tpch,
}

SYSTEM_TABLES_INFO_BY_NAME = {
    "SO": system_tables_info_so_10,
    "JOB": _load_resource_text("LLM_job_prompt_infos.txt"),
    "TPCH1": _load_resource_text("LLM_tpch1_prompt_infos.txt"),
    "TPCH10": _load_resource_text("LLM_tpch10_prompt_infos.txt"),
}

def get_schema_prompts(schema_name: str) -> tuple[str, str]:
    key = str(schema_name or "").strip().upper()
    if key not in SYSTEM_SCHEMA_BY_NAME or key not in SYSTEM_TABLES_INFO_BY_NAME:
        raise KeyError(f"Unknown schema name: {schema_name}")
    # print("Prompt stuff:")
    # print(SYSTEM_SCHEMA_BY_NAME[key])
    # print("****************************")
    # print(SYSTEM_TABLES_INFO_BY_NAME[key])

    return SYSTEM_SCHEMA_BY_NAME[key], SYSTEM_TABLES_INFO_BY_NAME[key]
