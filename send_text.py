import os
from twilio.rest import Client


# Your Account Sid and Auth Token from twilio.com/console
# and set the environment variables. See http://twil.io/secure

account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']
import os
from twilio.rest import Client


# Your Account Sid and Auth Token from twilio.com/console
# and set the environment variables. See http://twil.io/secure

account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']
phone_number_me=os.environ['phone_number_me']
phone_number_twilio=os.environ['phone_number_twilio']


def send_text(percent_change, price):
    client = Client(account_sid, auth_token)

    message = client.messages \
                .create(
                    body="\n BTC PRICE: "+ str(round(price,0))+"\n Volume: "+str(round(percent_change,3)*100)+"%",
                    from_=phone_number_twilio,
                    to=phone_number_me
                )

