import json
import boto3
import hashlib
from datetime import datetime

def read_file_from_s3(bucket_name, file_key):
 
    s3_client = boto3.client('s3')
    
    try:
        
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)

        file_content = response['Body'].read().decode('utf-8')
        
        return file_content
    except Exception as e:
        print(f"No se pudo leer el archivo {file_key} del bucket {bucket_name}: {str(e)}")
        return None

def convert_to_json(file_content):
   
    data = {}
    lines = file_content.split('\n')
    for line in lines:
        if '=' in line:
            key, value = line.strip().split('=')
            data[key] = int(value)
 
    json_data = json.dumps(data, indent=4)
    
    return json_data

def save_to_dynamodb(json_data):
    # cliente de DynamoDB
    dynamodb = boto3.resource('dynamodb')
    
   
    table = dynamodb.Table('ai-technical-test-hugo-steven')  
   
    timestamp = str(datetime.now())
    
    try:
       
        response = table.put_item(
            Item={
                'timestamp': timestamp,
                'data': json_data
            }
        )
        print("Datos guardados en DynamoDB correctamente.")
    except Exception as e:
        print(f"Error al guardar datos en DynamoDB: {str(e)}")

def delete_object_from_s3(bucket_name, file_key):
   
    s3_client = boto3.client('s3')
    
    try:
        # Eliminar el objeto del bucket de S3
        response = s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        print(f"Objeto {file_key} eliminado del bucket {bucket_name}.")
    except Exception as e:
        print(f"No se pudo eliminar el objeto {file_key} del bucket {bucket_name}: {str(e)}")

def generar_hash_md5(contenido):
    
    lineas = contenido.strip().split('\n')
    pares = [linea.split('=') for linea in lineas]

    hash_proporcionado=""
    string_base=""
    
    for clave, valor in pares:
        if clave.strip() == 'hash':
            hash_proporcionado=valor.strip() 
        else:
            
           string_base += valor.strip() + '~'
            
    if string_base.endswith('~'):
        string_base = string_base[:-1] 
    hash_calculado = hashlib.md5(string_base.encode()).hexdigest()

    return hash_proporcionado,  hash_calculado


def lambda_handler(event, context):
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    
    file_content = read_file_from_s3(bucket_name, file_key)
    
    if file_content is not None:
        # Convertir  JSON
        hash_proporcionado,  hash_calculado = generar_hash_md5(file_content)
        if hash_calculado == hash_proporcionado:
            json_data = convert_to_json(file_content)
            save_to_dynamodb(json_data)
            delete_object_from_s3(bucket_name, file_key)
        else:
            print("El hash calculado no coincide con el hash proporcionado en el archivo.")
         

    return {
        'statusCode': 200,
        'body': json.dumps('Procesamiento completado')
    }
