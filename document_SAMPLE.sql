
/* SOURCE */

alter procedure dbo.DOCUMENT_getid
@key nvarchar(32),
@retid bigint output
as
begin
	set nocount on;
	select top 1 @retid = D_ID from a2doc.DOCUMENTS where D_SENT=0 and D_ID<>0
end
go


alter procedure dbo.DOCUMENT_load
@key nvarchar(32),
@docid bigint
as
begin
	set nocount on;
	select TABLE_NAME=N'DOCUMENT', D_ID, D_DATE, D_SUM, D_MEMO from a2doc.DOCUMENTS where D_ID=@docid;
end
go

alter procedure dbo.DOCUMENT_written
@key nvarchar(32),
@docid bigint
as
begin
	set nocount on;
	update a2doc.DOCUMENTS set D_SENT=1 where D_ID=@docid;
end
go

/* SOURCE WITH GUID */

alter procedure dbo.DOCUMENT_getguid
@key nvarchar(32),
@retguid uniqueidentifier output
as
begin
	set nocount on;
	select top 1 @retguid = D_GUID from a2doc.DOCUMENTS where D_SENT=0 and D_ID<>0
end
go

alter procedure dbo.DOCUMENT_load
@key nvarchar(32),
@docguid uniqueidentifier
as
begin
	set nocount on;
	select TABLE_NAME=N'DOCUMENT', D_ID, D_DATE, D_SUM, D_MEMO from a2doc.DOCUMENTS where D_GUID=@docguid;
end
go

alter procedure dbo.DOCUMENT_written
@key nvarchar(32),
@docguid uniqueidentifier
as
begin
	set nocount on;
	update a2doc.DOCUMENTS set D_SENT=1 where D_GUID=@docguid;
end
go

/* TARGET */

alter procedure dbo.DOCUMENT_DOCUMENT_write
@TABLE_NAME nvarchar(32),
@D_ID bigint,
@D_DATE datetime,
@D_SUM money,
@D_MEMO nvarchar(255) = null
as
begin
	set nocount on;
	if exists(select * from a2doc.DOCUMENTS where D_ID=@D_ID)
		update a2doc.DOCUMENTS set D_DATE=@D_DATE, D_SUM=@D_SUM, D_MEMO=@D_MEMO where D_ID=@D_ID;
	else
		insert into a2doc.DOCUMENTS (D_ID, D_DATE, D_SUM, D_MEMO) values (@D_ID, @D_DATE, @D_SUM, @D_MEMO)
end
go


--update a2doc.DOCUMENTS set D_SENT=0