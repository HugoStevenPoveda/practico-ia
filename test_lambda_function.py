import unittest
import json
from unittest.mock import patch, MagicMock
from lambda_function import lambda_handler, read_file_from_s3, convert_to_json, save_to_dynamodb, delete_object_from_s3, generar_hash_md5

class TestLambdaFunction(unittest.TestCase):

    @patch('lambda_function.boto3.client')
    @patch('lambda_function.hashlib.md5')
    def test_lambda_handler(self, mock_md5, mock_s3_client):
        # Mock de evento
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'test-file.txt'}
                }
            }]
        }
        
        # Mock de contenido del archivo
        file_content = "totalContactoClientes=250\nmotivoReclamo=25\nmotivoGarantia=10\nmotivoDuda=100\nmotivoCompra=100\nmotivoFelicitaciones=7\nmotivoCambio=8\n"
        
        # Mock de respuesta de S3
        mock_s3_client.return_value.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=file_content))
        }
        
        # Mock de hashlib.md5
        mock_md5.return_value.hexdigest.return_value = "hash_calculado"
        
        # Mock de funciones internas
        read_file_from_s3_mock = MagicMock(return_value=file_content)
        convert_to_json_mock = MagicMock()
        save_to_dynamodb_mock = MagicMock()
        delete_object_from_s3_mock = MagicMock()
        
        # Ejecutar la función Lambda
        with patch('lambda_function.read_file_from_s3', read_file_from_s3_mock), \
             patch('lambda_function.convert_to_json', convert_to_json_mock), \
             patch('lambda_function.save_to_dynamodb', save_to_dynamodb_mock), \
             patch('lambda_function.delete_object_from_s3', delete_object_from_s3_mock):
            
            lambda_handler(event, None)
        
        # Verificar
        read_file_from_s3_mock.assert_called_once_with('test-bucket', 'test-file.txt')
        convert_to_json_mock.assert_called_once_with(file_content)
        save_to_dynamodb_mock.assert_called_once()
        delete_object_from_s3_mock.assert_called_once_with('test-bucket', 'test-file.txt')

    @patch('lambda_function.boto3.client')
    def test_read_file_from_s3(self, mock_s3_client):
        # Mock de respuesta de S3
        file_content = "totalContactoClientes=250\nmotivoReclamo=25\nmotivoGarantia=10\nmotivoDuda=100\nmotivoCompra=100\nmotivoFelicitaciones=7\nmotivoCambio=8\n"
        mock_s3_client.return_value.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=file_content))
        }

        # Llamar a la función
        content = read_file_from_s3('test-bucket', 'test-file.txt')

        # Verificar
        self.assertEqual(content, file_content)


def test_convert_to_json(self):
    # Datos de entrada
    file_content = "totalContactoClientes=250\nmotivoReclamo=25\nmotivoGarantia=10\nmotivoDuda=100\nmotivoCompra=100\nmotivoFelicitaciones=7\nmotivoCambio=8\n"
    expected_json = {
        'totalContactoClientes': 250,
        'motivoReclamo': 25,
        'motivoGarantia': 10,
        'motivoDuda': 100,
        'motivoCompra': 100,
        'motivoFelicitaciones': 7,
        'motivoCambio': 8
    }

    # Llamar a la función
    json_data = convert_to_json(file_content)

    # Verificar
    self.assertEqual(json.loads(json_data), expected_json)


    @patch('lambda_function.boto3.resource')
    def test_save_to_dynamodb(self, mock_dynamodb_resource):
        # Mock de DynamoDB
        table_mock = MagicMock()
        mock_dynamodb_resource.return_value.Table.return_value = table_mock

        # Datos de entrada
        json_data = {'test': 'data'}

        # Llamar a la función
        save_to_dynamodb(json_data)

        # Verificar
        table_mock.put_item.assert_called_once_with(Item={'timestamp': 'test_timestamp', 'data': json_data})

    @patch('lambda_function.boto3.client')
    def test_delete_object_from_s3(self, mock_s3_client):
        # Llamar a la función
        delete_object_from_s3('test-bucket', 'test-file.txt')

        # Verificar
        mock_s3_client.return_value.delete_object.assert_called_once_with(Bucket='test-bucket', Key='test-file.txt')

    def test_generar_hash_md5(self):
        # Datos de entrada
        contenido = "totalContactoClientes=250\nmotivoReclamo=25\nmotivoGarantia=10\nmotivoDuda=100\nmotivoCompra=100\nmotivoFelicitaciones=7\nmotivoCambio=8\nhash=2f941516446dce09bc2841da60bf811f"

        # Llamar a la función
        hash_proporcionado, hash_calculado = generar_hash_md5(contenido)

        # Verificar
        self.assertEqual(hash_proporcionado, "2f941516446dce09bc2841da60bf811f")
        self.assertEqual(hash_calculado, "2f941516446dce09bc2841da60bf811f")

if __name__ == '__main__':
    unittest.main()
