import imaplib
import email
from email.header import decode_header


class MailClient:

    mail = ''
    unseen_mails = ''

    def __init__(self, login, password, mail):
        self.mail = imaplib.IMAP4_SSL(f'imap.{mail}')
        self.mail.login(login, password)

    def get_all_folders(self):
        return self.mail.list()

    def search_unseen_mails_in_folder(self, folder):
        self.mail.select(folder)
        unseen_mails = self.mail.uid('search', 'UNSEEN')[1][0].decode().split(' ')
        if len(unseen_mails) == 1 and unseen_mails[0] == '':
            self.unseen_mails = []
        else:
            self.unseen_mails = unseen_mails
        return self.unseen_mails

    def download_mail_attach(self, message_num, mask=''):
        res, msg = self.mail.uid('fetch', message_num, '(RFC822)')
        message = email.message_from_bytes(msg[0][1])
        # print(message)
        for part in message.walk():
            if part.get_content_disposition() == 'attachment':
                # filename_parts = part.get_filename().split('?')
                # filename = base64.b64decode(filename_parts[3]).decode('utf-8')
                filename = part.get_filename()
                filename = str(email.header.make_header(email.header.decode_header(filename)))
                if mask in filename:
                    with open(filename, 'wb') as new_file:
                        new_file.write(part.get_payload(decode=True))
                    print(f'{filename} сохранен')

    def logout(self):
        self.mail.logout()


# Создаем объект класса, сразу логинимся
mail_ru = MailClient(login='support3@a-lain.ru', password='4NkfhZ1k1th8WtBt3MGg', mail='mail.ru')
# Вывод всех папок
# print(mail_ru.get_all_folders())
# Получаем непрочитанные письма в определенной папке
unseen_mails = mail_ru.search_unseen_mails_in_folder('inbox')
# Для каждого из непрочиттанных писем скачиваем вложение формата .xlsx
# (указывать формат не обязательно, тогда будут скачиваться все вложения)
for mail in unseen_mails:
    mail_ru.download_mail_attach(mail, '.xlsx')
# Выходим из почты
mail_ru.logout()
del mail_ru
