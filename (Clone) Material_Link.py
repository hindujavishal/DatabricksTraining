# Databricks notebook source
https://drive.google.com/drive/folders/1I4ZjKa6SJvN-WiYQAa-UVqkCRcn0-kh9?usp=sharing

# COMMAND ----------

# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.install('sql-ai-functions', catalog='main', schema='dbdemos_ai_query')

# COMMAND ----------

import dbdemos
dbdemos.install('llm-rag-chatbot', catalog='main', schema='rag_chatbot')

# COMMAND ----------


