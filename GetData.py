
# for aws dependencies
import boto3
import json
import uuid
# for email
import smtplib
import ssl
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
# for data collection and graphs
import requests
import matplotlib.pyplot as plt
from datetime import date
from datetime import datetime
from bs4 import BeautifulSoup
from boto3.dynamodb.conditions import Key

senderEmail = 'test@email.com'
senderUn = 'un'
senderPass = 'pw'
countyList = [
        'Albany',
        'Big Horn',
        'Campbell',
        'Carbon',
        'Converse',
        'Crook',
        'Fremont',
        'Goshen',
        'Hot Springs',
        'Johnson',
        'Laramie',
        'Lincoln',
        'Natrona',
        'Niobrara',
        'Park',
        'Platte',
        'Sheridan',
        'Sublette',
        'Sweetwater',
        'Teton',
        'Uinta',
        'Washakie',
        'Weston'
    ]

# testing purposes only
#def __init__(self):
#    main()

def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Insert from Lambda success!')
    }

def put_CountiesAndCases(countiesAndCases, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('WyomingCoronavirusCountyCaseCount')
    for key in countiesAndCases:
        rowUuid = str(uuid.uuid4())
        response = table.put_item(
        Item={
                'WyomingCoronavirusCountyCaseCountId' : rowUuid,
                'county': key,
                'cases': countiesAndCases[key][1][0],
                'date' : countiesAndCases[key][0][0]
            }
        )
    return response

def get_wyoming_data():
    htmlBody = getHtmlAsString('https://health.wyo.gov/publichealth/infectious-disease-epidemiology-unit/disease/novel-coronavirus/')
    countyAndCases = parseCountiesAndCounts(htmlBody, countyList)
    return countyAndCases

def get_wyoming_previous_data(datum_to_append=None, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('WyomingCoronavirusCountyCaseCount')

    # this table design was bad and I feel bad :(
    # at least its just a prototype project to get used to dynamoDB & Lambda
    # I really hope coronavirus doesn't last long enough to make this 
    # super expensive :o
    response = table.scan(ProjectionExpression="county,#dateIsAReservedKeyword,cases",
        ExpressionAttributeNames={'#dateIsAReservedKeyword': 'date'})
    countyData = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ProjectionExpression="county,#dateIsAReservedKeyword,cases",
            ExpressionAttributeNames={'#dateIsAReservedKeyword': 'date'},
            ExclusiveStartKey=response['LastEvaluatedKey'])
        countyData.extend(response['Items'])

    # dont think about this too hard, like I mentioned earlier,
    # this weirdness stems from my poor initial table design 
    # stemming from my lack of understanding of DynamoDB
    formattedData = dict()
    for countydatum in countyData:
        if not countydatum:
            continue
        county = countydatum['county']
        if not formattedData.get(county):
            formattedData[county] = [[countydatum['date']],[countydatum['cases']]]
        else:
            formattedData[county][0].append(countydatum['date'])
            formattedData[county][1].append(countydatum['cases'])

    if(datum_to_append is not None):
        for county in datum_to_append:
            formattedData[county][0].extend(datum_to_append[county][0])
            formattedData[county][1].extend(datum_to_append[county][1])
    
    return formattedData


def data_exists_for_today(dynamodb=None):
    today = str(date.today())
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('WyomingCoronavirusCountyCaseCount')

    # this table design was bad and I feel bad :(
    # at least its just a prototype project to get used to dynamoDB & Lambda
    # I really hope coronavirus doesn't last long enough to make this 
    # super expensive :o
    response = table.scan(ProjectionExpression="#dateIsAReservedKeyword", 
        ExpressionAttributeNames={'#dateIsAReservedKeyword': 'date'})
    result = response['Items']
    for dataDate in result:
        if dataDate == today:
            return True
    while 'LastEvaluatedKey' in response:
        response = table.scan(ProjectionExpression="#dateIsAReservedKeyword", 
            ExpressionAttributeNames={'#dateIsAReservedKeyword': 'date'}, 
            ExclusiveStartKey=response['LastEvaluatedKey'])
        result = response['Items']
        for dataDate in result:
            if dataDate == today:
                return True

    return False


def get_emails(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    # I've been collecting data in this one table for a while
    # and I didn't expect it to also have email data in it
    # so the table is admittedly poorly named now :(
    # regular data should just be a uuid
    # email addresses should be #email#{uuid}
    table = dynamodb.Table('WyomingCoronavirusCountyCaseCount')
    response = table.query(
        IndexName="emailKey-index",
        KeyConditionExpression=Key('emailKey').eq('email')
    )

    #email objects should look like 
    #{
    #    "WyomingCoronavirusCountyCaseCountId" : "address@email.com",
    #    "emailKey" : "email"
    #    "email" : "address@email.com"
    #}

    emailList = []
    for email in response['Items']:
        emailList.append(email['email'])

    return emailList


def create_graphs(countyAndCaseData):
    filenames = []
    for county in countyAndCaseData:
        # make a new graph!
        # handle dates
        xaxis = []
        for stringDate in countyAndCaseData[county][0]:
            xval = datetime.strptime(stringDate, '%Y-%m-%d')
            xaxis.append(xval)
        yaxis = countyAndCaseData[county][1] # cases
        # order by date for graphs
        xaxis, yaxis = zip(*sorted(zip(xaxis, yaxis)))

        fig = plt.figure()
        plt.plot(xaxis, yaxis, label=county)
        plt.title(county)
        plt.xlabel('Date') 
        plt.xticks(rotation=15)
        plt.ylabel('Number Of Cases') 
        plt.tight_layout() # make sure x axis label doesn't get cut off
        # filename = '/tmp/' + str(date.today()) + county + '.png'
        # In older versions, I appended the date so that I could better keep track
        # of the filenames. Now I'm making it generic so that I can overwrite the files 
        # for my website and always have fresh data
        filename = '/tmp/' + county + '.png'
        filenames.append(filename)
        fig.savefig(filename)
        fig.clear()
        plt.close(fig)
    
    return filenames


def main():
    print('main')
    todaysCountyAndCases = get_wyoming_data() # dictionary of county and case count
    print('got wyoming data')
    # -- begin testing purposes only
    #print(todaysCountyAndCases['Albany'][0])
    #print(todaysCountyAndCases['Albany'][1])
    #filenames = create_graphs(todaysCountyAndCases)
    #emailList = ["test@email.com"]
    # -- end testing purposes only

    countyAndCaseData = dict()
    print('getting data from db')
    if not data_exists_for_today():
        print('data did not exist for today')
        put_CountiesAndCases(todaysCountyAndCases)
        print('added todays data')
        countyAndCaseData = get_wyoming_previous_data(todaysCountyAndCases)
        print('got previous data')
    else:
        countyAndCaseData = get_wyoming_previous_data() # dictionary of date and dictionary of county and case count
        print('got previous data')
    
    filenames = create_graphs(countyAndCaseData)
    print('created graphs')

    # upload files to s3
    target_bucket = 'luisorlandogarcia.com-images'
    bucket_dir = 'technical/coronavirus-metrics/wyoming'
    upload_to_s3(filenames, target_bucket, bucket_dir)
    print('uploaded to s3')

    #emailList = get_emails()
    #print('got email list')
    #email_data(emailList, filenames)
    #print('done')

def upload_to_s3(filenames, bucket, bucket_dir):
    s3_client = boto3.client('s3')
    for file in filenames:
        only_filename = file.split('/')[-1]
        s3_client.upload_file(file, bucket, f"{bucket_dir}/{only_filename}")

def email_data(emailList, filenames):

    # there is a more efficient and better way to do this
    # the create_email_message opens each image and attaches it
    # since I'm assuming the emailList will be small (< 10 emails)
    # this loop and create_email_message should be fine for now
    message = create_email_message(filenames)
    message["Subject"] = 'Wyoming Coronavirus Case Graphs'
    message["From"] = senderEmail
    messageTo = ''
    for email in emailList:
        messageTo = email + ','
    messageTo = messageTo.strip(',')
    message["To"] = messageTo

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("email-smtp.us-east-1.amazonaws.com", 465, context=context) as server:
        server.login(senderUn, senderPass)
        server.sendmail(senderEmail, messageTo, message.as_string())

    return

def create_email_message(filenames):
    message = MIMEMultipart()
    messageHtml = '<h2>Wyoming Coronavirus Cases by County</h2><b></b><p>' + str(date.today()) + '</p>'
    messageHtml = messageHtml + '<div>'
    for filename in filenames:
        # attach image
        fp = open(filename, 'rb')                                                    
        img = MIMEImage(fp.read())
        fp.close()
        img.add_header('Content-ID', '<{}>'.format(filename))
        message.attach(img)

        # deal with html
        imageTag = '<img src="cid:%s">' % (filename)
        messageHtml = messageHtml + imageTag

    messageHtml = messageHtml + '</div>'
    message.attach(MIMEText(messageHtml, 'html'))
    return message

def getHtmlAsString(url):
    s = requests.Session()
    headers = {
            'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
        }
    page = s.get(url, headers=headers)
    content = page.content.decode("utf-8") 
    return content


def parseCountiesAndCounts(htmlBody, countyList):
    soup = BeautifulSoup(htmlBody, 'html.parser')
    allps = soup.find_all('p')
    countyAndCases = dict()
    for element in allps:
        if element.contents is None:
            continue
        # this is a little weird, the first county is in the <p>. All subsequent counties are in <br>'s within the <p>
        for child in element.contents: 
            str_data = child.string
            if str_data is None:
                continue
            splitCounty = str_data.split(':')
            if not (splitCounty[0] in countyList):
                continue
            county = splitCounty[0]
            cases = splitCounty[1].split()[0]
            cases = cases.replace(',', '')
            countyAndCases[county] = [[str(date.today())],[int(cases)]]

    return countyAndCases
    

def parseCountyBlock(unparsed_counties) -> dict:
    countyAndCases = dict()
    for unparsedCounty in unparsed_counties:
        elementValue = unparsedCounty.text
        splitCounty = elementValue.split(':')
        if not (splitCounty[0] in countyList):
            continue
        county = splitCounty[0]
        cases = splitCounty[1].split()[0]
        countyAndCases[county] = [[str(date.today())],[int(cases)]]

    return countyAndCases


#Outdated as of 10-21-2020
def parseCountiesAndCounts_legacy(htmlBody, countyList):
    soup = BeautifulSoup(htmlBody, 'html.parser')
    unparsedCounties = soup.find_all('strong')

    return parseCountyBlock(unparsedCounties)


if __name__ == "__main__":
    main()