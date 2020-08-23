
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
from bs4 import BeautifulSoup
from boto3.dynamodb.conditions import Key

senderEmail = 'mail@gmail.com'
senderPass = 'mycoolpass'
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

def get_wyoming_previous_data(datum_to_append=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('WyomingCoronavirusCountyCaseCount')

    formattedData = dict()
    for county in countyList:
        countyData = table.query(
            KeyConditionExpression=Key('county').eq(county)
        )
        dates = [] # should go on the x axis
        cases = [] # should go on the y axis
        for countydatum in countyData:
            dates.append(countydatum['date'])
            cases.append(countydatum['cases'])
        formattedData[county] = [dates,cases]

    if(datum_to_append is not None):
        for county in datum_to_append:
            formattedData[county][0].extend(datum_to_append[county][0])
            formattedData[county][1].extend(datum_to_append[county][1])
    
    return formattedData


def data_exists_for_today():
    today = str(date.today())
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('WyomingCoronavirusCountyCaseCount')
    dataForToday = table.query(
        KeyConditionExpression=Key('date').eq(today)
    )
    if dataForToday is None:
        return False
    else:
        return True


def get_emails():
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    # I've been collecting data in this one table for a while
    # and I didn't expect it to also have email data in it
    # so the table is admittedly poorly named now :(
    # regular data should just be a uuid
    # email addresses should be #email#{uuid}
    table = dynamodb.Table('WyomingCoronavirusCountyCaseCount')
    emails = table.query(
        KeyConditionExpression=Key('WyomingCoronavirusCountyCaseCountId').begins_with('#email')
    )

    #email objects should look like 
    #{
    #    "WyomingCoronavirusCountyCaseCountId" : "#email#{uuid}",
    #    "email" : "address@email.com"
    #}

    emailList = []
    for email in emails:
        emailList.append(emails['email'])

    return emailList


def create_graphs(countyAndCaseData):
    filenames = []
    for county in countyAndCaseData:
        # make a new graph!
        xaxis = countyAndCaseData[county][0] # dates
        yaxis = countyAndCaseData[county][1] # cases
        fig = plt.figure()
        plt.plot(xaxis, yaxis, label=county)
        plt.title(county)
        plt.xlabel('Date') 
        plt.ylabel('Number Of Cases') 
        filename = str(date.today()) + county + '.png'
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
    #emailList = ["ijollyman@yahoo.com"]
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
    emailList = get_emails()
    print('got email list')
    email_data(emailList, filenames)
    print('done')

def email_data(emailList, filenames):

    # there is a more efficient and better way to do this
    # the create_email_message opens each image and attaches it
    # since I'm assuming the emailList will be small (< 10 emails)
    # this loop and create_email_message should be fine for now
    message = create_email_message(filenames)
    message["Subject"] = 'Wyoming Coronavirus Case Graphs'
    message["From"] = 'luisa1b2c3d4@gmail.com'
    messageTo = ''
    for email in emailList:
        messageTo = email + ','
    messageTo = messageTo.strip(',')
    message["To"] = messageTo

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(senderEmail, senderPass)
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
    unparsedCounties = soup.find_all('strong')

    countyAndCases = dict()
    for unparsedCounty in unparsedCounties:
        elementValue = unparsedCounty.text
        splitCounty = elementValue.split(':')
        if not (splitCounty[0] in countyList):
            continue
        county = splitCounty[0]
        cases = splitCounty[1].split()[0]
        countyAndCases[county] = [[str(date.today())],[int(cases)]]

    return countyAndCases


if __name__ == "__main__":
    main()