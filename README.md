# update lambda function
zip function.zip lambda_function.py 
aws lambda update-function-code --function-name mongoDBConnection --zip-file fileb://function.zip


# create lambda function
aws lambda create-function --function-name mongoDBConnection  --zip-file fileb://function.zip --role arn:aws:iam::594664279561:role/mongoDBConnectionRole --runtime python3.8 --handler lambda_function.lambda_handler



