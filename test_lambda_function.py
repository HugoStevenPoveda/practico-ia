import unittest
import json
from unittest.mock import patch, MagicMock
from lambda_function import lambda_handler, read_file_from_s3, convert_to_json, save_to_dynamodb, delete_object_from_s3

class TestLambdaFunction(unittest.TestCase):

    @patch('lambda_function.boto3.client')
    def test_lambda_handler(self, mock_s3_client):
       
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'test-file.txt'}
                }
            }]
        }
        
        file_content = "totalContactoClientes=250\nmotivoReclamo=25\nmotivoGarantia=10\nmotivoDuda=100\nmotivoCompra=100\nmotivoFelicitaciones=7\nmotivoCambio=8\n"
     
        mock_s3_client.return_value.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=file_content))
        }
        
        # funciones internas
        read_file_from_s3_mock = MagicMock(return_value=file_content)
        convert_to_json_mock = MagicMock(return_value=json.dumps({'totalContactoClientes': 250, 'motivoReclamo': 25, 'motivoGarantia': 10, 'motivoDuda': 100, 'motivoCompra': 100, 'motivoFelicitaciones': 7, 'motivoCambio': 8}, indent=4))
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
        save_to_dynamodb_mock.assert_called_once_with(json.dumps({'totalContactoClientes': 250, 'motivoReclamo': 25, 'motivoGarantia': 10, 'motivoDuda': 100, 'motivoCompra': 100, 'motivoFelicitaciones': 7, 'motivoCambio': 8}, indent=4))
        delete_object_from_s3_mock.assert_called_once_with('test-bucket', 'test-file.txt')

if __name__ == '__main__':
    unittest.main()
