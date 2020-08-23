# Wyoming Coronavirus Metrics
This is an AWS Lambda function that reads coronavirus data from the Wyoming Department of health website, stores it in a DynamoDB table. It then generates graphs for each county and emails the graphs to user emails also stored in the DynamoDB table.

# Deployment
1. make sure GetData.py has the correct password for the SMTP server
2. Create deployment folder 'deploy'
3. Install dependencies (dependencies are noted in requirements.txt)
    pip install -r requirements.txt -t ./deploy
4. Copy GetData.py into deployment folder
5. zip deployment folder 
    zip -r /deploy.zip .
6. Upload to AWS