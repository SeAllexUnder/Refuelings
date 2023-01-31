import imaplib
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import decode_header


class MailClient:

    mail = ''
    unseen_mails = ''
    login = ''
    password = ''

    def __init__(self, login, password, mail):
        self.mail = imaplib.IMAP4_SSL(f'imap.{mail}')
        self.login = login
        self.password = password
        self.mail.login(login, password)

    def get_all_folders(self):
        return self.mail.list()[1]

    def search_unseen_mails_in_folder(self, folder):
        self.mail.select(folder)
        unseen_mails = self.mail.uid('search', 'UNSEEN')[1][0].decode().split(' ')
        if len(unseen_mails) == 1 and unseen_mails[0] == '':
            self.unseen_mails = []
        else:
            self.unseen_mails = unseen_mails
        return self.unseen_mails

    def download_mail_attach(self, message_num, mask='', mail_from = ''):
        files = []
        res, msg = self.mail.uid('fetch', message_num, '(RFC822)')
        message = email.message_from_bytes(msg[0][1])
        field_from = decode_header(message['From'])[1][0].decode()
        if mail_from in field_from or 'support3@a-lain.ru' in field_from:
            for part in message.walk():
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    filename = str(email.header.make_header(email.header.decode_header(filename)))
                    if mask in filename:
                        files.append(filename)
                        with open(filename, 'wb') as new_file:
                            new_file.write(part.get_payload(decode=True))
                        text = 'Файл' + f' <b>{filename}</b> ' + \
                               'принят в обработку. О заврешении обработки будет сообщено дополнительно'
                        self.send_message(text_for_send=text, mail_to=field_from, subject=f'Файл {filename} принят.')

        return files

    def send_message(self, text_for_send, subject, mail_to):
        msg = MIMEMultipart()
        msg['From'] = self.login
        msg['To'] = mail_to
        msg['Subject'] = subject

        signature = 'Сообщение сформировано и отправлено автоматически и не ответа не требует. Если у Вас возникли ' \
                    'вопросы по содержанию данного письма, пожалуйста, напишите обращение на support3@a-lain.ru'
        body = f'{text_for_send}'+ '<br>'*2 + ('<b>' + signature + '</b>')
        print(body)
        msg.attach(MIMEText(body, 'html'))

        server = smtplib.SMTP_SSL('smtp.mail.ru', 465)
        # server.starttls()
        server.login(self.login, self.password)
        text = msg.as_string()
        server.sendmail(self.login, mail_to, text)
        server.quit()

    def logout(self):
        self.mail.logout()


# Создаем объект класса, сразу логинимся
mail_ru = MailClient(login='report2@a-lain.ru', password='eHLYRrFsMPnsHz4ggu1d', mail='mail.ru')

# Вывод всех папок
# print(mail_ru.get_all_folders())

# Получаем непрочитанные письма в определенной папке (целевая папка должна быть без пробелов!)
unseen_mails = mail_ru.search_unseen_mails_in_folder('ATL_Novatec')

# Для каждого из непрочиттанных писем скачиваем вложение формата .xlsx
# (указывать формат не обязательно, тогда будут скачиваться все вложения)
for mail in unseen_mails:
    mail_ru.download_mail_attach(message_num=mail, mask='.xlsx', mail_from='noleinayk@atl.biz')

# mail_ru.send_message('test', 'support3@a-lain.ru')

# Выходим из почты
mail_ru.logout()

del mail_ru
