--路口代价hbase存储表
create table if not exists "CROSSROADPRICE"(
"ROADIDONE" INTEGER NOT NULL,
"ROADIDTWO" INTEGER NOT NULL,
"DAYHOUR" INTEGER NOT NULL,
"ROADPRICE"."TIME" DOUBLE,
constraint dos_pk primary key("ROADIDONE", "ROADIDTWO", "DAYHOUR"));

--从hive根据出入路口的linkIdId查询数据
select case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.lon') as Double) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.lon') as Double) end as roadIdTrackOneLon1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.lon') as Double) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double) end as roadIdTrackOneLat1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.DEVtimestamp') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.DEVtimestamp') as bigint) end as roadIdTrackOneDEVtimestamp1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.GPStimestamp') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.GPStimestamp') as bigint) end as roadIdTrackOneGPStimestamp1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.speed') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.speed') as Double) end as roadIdTrackOneSpeed1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double)as roadIdTrackTwoLon1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double)as roadIdTrackTwoLat1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.DEVtimestamp') as bigint)as roadIdTrackTwoDEVtimestamp1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.GPStimestamp') as bigint)as roadIdTrackTwoGPStimestamp1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.speed') as Double)as roadIdTrackTwoSpeed1 from dw_tbtravel " +
        s"WHERE roadId LIKE '%${roadIdOne}%' AND roadId LIKE '%${roadIdTwo}%'
