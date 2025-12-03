#!/usr/bin/python3
# -*- coding: UTF-8 -*-
# 百炼知识库操作类
import requests
import json
import os
import time
import sys
from pathlib import Path
from typing import List
import logging
import logging.handlers
from pathlib import Path
import hashlib

from alibabacloud_bailian20231229.client import Client as bailian20231229Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_bailian20231229 import models as bailian_20231229_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient

# 生成文件hash
def CalcFileMD5(filepath):
    try:
        with open(filepath,'rb') as f:
            md5obj = hashlib.md5()
            md5obj.update(f.read())
            hash = md5obj.hexdigest()
            return hash
    except Exception as e:
        print(f"CalcFileMD5 for {filepath} fail, {e}")
    return None

# 生成文件的字节数据的hash
def CalcFileMD5FromBytes(bytes_data):
    try:
        md5obj = hashlib.md5()
        md5obj.update(bytes_data)
        hash = md5obj.hexdigest()
        return hash
    except Exception as e:
        print(f"CalcFileMD5FromBytes fail, {e}")
    return None

class BailianLKE:
    def __init__(self, secret_id, secret_key, WorkspaceId, endpoint=None):
        # 初始化日志
        self.logger = logging.getLogger('BailianLKE')
        self.logger.setLevel(logging.INFO)

        # 创建百炼SDK Client        
        self.endpoint = endpoint
        if endpoint is None:
            self.endpoint = 'bailian.cn-beijing.aliyuncs.com'
        self.secret_id = secret_id
        self.secret_key = secret_key
        self.WorkspaceId = WorkspaceId
        config = open_api_models.Config(
            access_key_id=secret_id,
            access_key_secret=secret_key
        )
        config.endpoint = self.endpoint
        self.client = bailian20231229Client(config)

    # 上传文档至百炼临时存储
    def upload_file(self, pre_signed_url, file_path, upload_lease):
        try:
            # 设置请求头
            headers = {
                "X-bailian-extra": upload_lease['Param']['Headers']['X-bailian-extra'],
                "Content-Type": upload_lease['Param']['Headers']['Content-Type'],
            }

            # 读取文档并上传
            with open(file_path, 'rb') as file:
                # 下方设置请求方法用于文档上传，需与您在上一步中调用ApplyFileUploadLease接口实际返回的Data.Param中Method字段的值一致
                response = requests.put(pre_signed_url, data=file, headers=headers)

            # 检查响应状态码
            if response.status_code == 200:
                self.logger.info(f"File {file_path} uploaded successfully.")
            else:
                raise Exception(f"Failed to upload the file {file_path}. ResponseCode: {response.status_code}")

        except Exception as e:
            msg = f"upload_file fail, {e}"
            self.logger.error(msg)
            raise Exception(msg)
        
    # 上传文档的字节数据至百炼临时存储
    def upload_file_from_bytesio(self, pre_signed_url, uploaded_file, upload_lease):
        try:
            # 设置请求头
            headers = {
                "X-bailian-extra": upload_lease['Param']['Headers']['X-bailian-extra'],
                "Content-Type": upload_lease['Param']['Headers']['Content-Type'],
            }

            # 读取文档并上传
            # 下方设置请求方法用于文档上传，需与您在上一步中调用ApplyFileUploadLease接口实际返回的Data.Param中Method字段的值一致
            response = requests.put(pre_signed_url, data=uploaded_file, headers=headers)

            # 检查响应状态码
            if response.status_code == 200:
                self.logger.info(f"File {uploaded_file.name} uploaded successfully.")
            else:
                raise Exception(f"Failed to upload the file {uploaded_file.name}. ResponseCode: {response.status_code}")

        except Exception as e:
            msg = f"upload_file_from_bytesio fail, {e}"
            self.logger.error(msg)
            raise Exception(msg)

    # 上传文档到百炼数据管理，上传成功则返回FileId
    def TransferDocument(self, localfile, tags, CategoryId):
        runtime = util_models.RuntimeOptions()
        headers = {}

        try:
            apply_file_upload_lease_request = bailian_20231229_models.ApplyFileUploadLeaseRequest(
                file_name=Path(localfile).name,
                md_5=CalcFileMD5(localfile),
                size_in_bytes=os.path.getsize(localfile)
            )
            # 申请文档上传租约
            resp = self.client.apply_file_upload_lease_with_options(CategoryId, self.WorkspaceId, apply_file_upload_lease_request, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"apply_file_upload_lease_with_options fail, {result}")
            upload_lease = result['body']['Data']

            # 上传文档至临时存储
            self.upload_file(upload_lease['Param']['Url'], localfile, upload_lease)

            # 将上传的文档添加至百炼数据管理
            add_file_request = bailian_20231229_models.AddFileRequest(
                lease_id=upload_lease['FileUploadLeaseId'],
                parser='DASHSCOPE_DOCMIND',
                category_id=CategoryId,
                tags=tags
            )
            resp = self.client.add_file_with_options(self.WorkspaceId,
                                        add_file_request, 
                                        headers, 
                                        runtime)    
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"add_file_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"TransferDocument {localfile} fail, {e}, tags={tags}"
            self.logger.error(msg)
            raise Exception(msg)
        return None

    # 上传Streamlit的UploadedFile文档到百炼数据管理，上传成功则返回FileId
    def TransferUploadedFileFromStreamlit(self, uploaded_file, tags, CategoryId):
        runtime = util_models.RuntimeOptions()
        headers = {}

        try:
            apply_file_upload_lease_request = bailian_20231229_models.ApplyFileUploadLeaseRequest(
                file_name=uploaded_file.name,
                md_5=CalcFileMD5FromBytes(uploaded_file.getvalue()),
                size_in_bytes=uploaded_file.size
            )
            # 申请文档上传租约
            resp = self.client.apply_file_upload_lease_with_options(CategoryId, self.WorkspaceId, apply_file_upload_lease_request, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"apply_file_upload_lease_with_options fail, {result}")
            upload_lease = result['body']['Data']

            # 上传文档至临时存储
            self.upload_file_from_bytesio(upload_lease['Param']['Url'], uploaded_file, upload_lease)

            # 将上传的文档添加至百炼数据管理
            add_file_request = bailian_20231229_models.AddFileRequest(
                lease_id=upload_lease['FileUploadLeaseId'],
                parser='DASHSCOPE_DOCMIND',
                category_id=CategoryId,
                tags=tags
            )
            resp = self.client.add_file_with_options(self.WorkspaceId,
                                        add_file_request, 
                                        headers, 
                                        runtime)    
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"add_file_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"TransferDocument {uploaded_file.name} fail, {e}, tags={tags}"
            self.logger.error(msg)
            raise Exception(msg)
        return None

    # 查询文档状态
    def DescribeDocument(self, FileId):
        runtime = util_models.RuntimeOptions()
        headers = {}
        try:
            resp = self.client.describe_file_with_options(self.WorkspaceId, FileId, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"describe_file_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"DescribeDocument fail, {e}"
            self.logger.error(msg)
            raise Exception(msg)
    
    # 文档列表
    def ListDocuments(self, CategoryId, MaxResults=50, FileName=None, NextToken=None):
        runtime = util_models.RuntimeOptions()
        headers = {}
        try:
            req = bailian_20231229_models.ListFileRequest(
                category_id=CategoryId,
                max_results=MaxResults,
                next_token=NextToken,
                file_name=FileName
            )            
            resp = self.client.list_file_with_options(self.WorkspaceId, req, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"list_file_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"ListDocuments fail, {e}"
            self.logger.error(msg)
            raise Exception(msg)

    # 文档分类列表
    def ListCategory(self, ParentCategoryId=None, MaxResults=200, NextToken=None):
        runtime = util_models.RuntimeOptions()
        headers = {}
        try:
            req =bailian_20231229_models.ListCategoryRequest(
                category_type='UNSTRUCTURED',
                parent_category_id=ParentCategoryId,
                next_token=NextToken,
                max_results=MaxResults
            )            
            resp = self.client.list_category_with_options(self.WorkspaceId, req, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"list_category_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"ListCategory fail, {e}"
            self.logger.error(msg)
            raise Exception(msg)

    # 将文档从百炼数据管理删除
    def DeleteDocument(self, FileId):
        runtime = util_models.RuntimeOptions()
        headers = {}
        try:
            resp = self.client.delete_file_with_options(FileId, self.WorkspaceId, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"delete_file_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"DeleteDocument fail, {e}"
            self.logger.error(msg)
            # raise Exception(msg)
        return None

    # 将文档加入知识库索引
    def AddDocumentsToIndex(self, IndexId, DocumentIds):
        runtime = util_models.RuntimeOptions()
        headers = {}
        try:
            req = bailian_20231229_models.SubmitIndexAddDocumentsJobRequest(
                index_id=IndexId,
                source_type='DATA_CENTER_FILE',
                document_ids=DocumentIds
            )            
            resp = self.client.submit_index_add_documents_job_with_options(self.WorkspaceId, req, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"submit_index_add_documents_job_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"AddDocumentsToIndex fail, {e}"
            self.logger.error(msg)
            raise Exception(msg)

    # 查询知识索引下的文档列表
    def ListIndexDocuments(self, IndexId, DocumentStatus=None, PageNumber=1, PageSize=100):
        runtime = util_models.RuntimeOptions()
        headers = {}
        try:
            req = bailian_20231229_models.ListIndexDocumentsRequest(
                index_id=IndexId,
                page_number=PageNumber,
                page_size=PageSize,
                document_status=DocumentStatus
            )            
            resp = self.client.list_index_documents_with_options(self.WorkspaceId, req, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"list_index_documents_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"ListIndexDocuments fail, {e}"
            self.logger.error(msg)
            raise Exception(msg)

    # 将文档从知识库索引删除
    def DeleteDocumentsFromIndex(self, IndexId, DocumentIds):
        runtime = util_models.RuntimeOptions()
        headers = {}
        try:
            req =  bailian_20231229_models.DeleteIndexDocumentRequest(
                index_id=IndexId,
                document_ids=DocumentIds
            )   
            resp = self.client.delete_index_document_with_options(self.WorkspaceId, req, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"delete_index_document_with_options fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"DeleteDocumentsFromIndex fail, {e}"
            self.logger.error(msg)
            # raise Exception(msg)
        return None

    # 创建知识库索引并将文档加入知识库
    def CreateIndex(self, 
                    IndexName, 
                    StructureType='unstructured', 
                    EmbeddingModelName='text-embedding-v2',
                    RerankModelName='gte-rerank-hybrid',
                    RerankMinScore=0.6,
                    ChunkSize=6000,
                    OverlapSize=100,
                    Separator='',
                    SourceType='DATA_CENTER_FILE',
                    DocumentIds=[],
                    SinkType='BUILT_IN'):
        runtime = util_models.RuntimeOptions()
        headers = {}
        try:
            req = bailian_20231229_models.CreateIndexRequest(
                name=IndexName,
                structure_type=StructureType,
                embedding_model_name=EmbeddingModelName,
                rerank_model_name=RerankModelName,
                chunk_size=ChunkSize,
                overlap_size=OverlapSize,
                source_type=SourceType,
                document_ids=DocumentIds,
                sink_type=SinkType,
                separator=Separator
            )   
            resp = self.client.create_index_with_options(self.WorkspaceId, req, headers, runtime)
            result = json.loads(UtilClient.to_jsonstring(resp))
            if not result['body']['Success']:
                raise Exception(f"create_index_with_options_async fail, {result}")
            return result['body']['Data']
        except Exception as e:
            msg = f"CreateIndex fail, {e}"
            self.logger.error(msg)
            raise Exception(msg)



