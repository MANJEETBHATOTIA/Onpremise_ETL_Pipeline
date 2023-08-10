import smtplib
import logging 
from email.message import EmailMessage

class email_notification:
    
        
    gmail_user = ''
    gmail_password = ''
        
        
    def email_properties(self):
        
        self.sent_from = self.gmail_user
        self.to = ['ymanjeet936@gmail.com']
        self.subject = 'ETl_project'
        
        
        
    def email_msg(self,msg=''):
        self.email_properties()
        self.msg = msg
        return msg 
        
    # def email_send_requests(self,enter_msg):
    #     self.email_msg(enter_msg)
    #     try:
    #         server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    #         server.ehlo()
    #         server.login(self.gmail_user, self.gmail_password)
    #         server.sendmail(self.sent_from,self.to,self.msg)
    #         server.close()  
    #         print ('Email sent!')
            
    #     except Exception as e:
    #         print(e)
    #         print ('Something went wrong...')
    
    def email_send_requests(self, enter_msg):
        self.email_msg(enter_msg)
        try:
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.ehlo()
            server.login(self.gmail_user, self.gmail_password)
            
            # Create the email message
            msg = EmailMessage()
            msg['Subject'] = self.subject
            msg['From'] = self.sent_from
            msg['To'] = ', '.join(self.to)
            msg.set_content(self.msg)
            
            # Send the email message
            server.send_message(msg)
            server.close()  
            print ('Email sent!')
        
        except Exception as e:
            print(e)
            print ('Something went wrong...')
            
                
                
email_notify = email_notification()
email_notify.email_msg()

