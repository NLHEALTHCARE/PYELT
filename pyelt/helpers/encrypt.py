import base64



"""simple Vigenere cipher encryption"""


class SimpleEncrypt():
    @staticmethod
    def encode(key, clear):
        key = SimpleEncrypt.__get_key() + key
        enc = []
        for i in range(len(clear)):
            key_c = key[i % len(key)]
            enc_c = chr((ord(clear[i]) + ord(key_c)) % 256)
            enc.append(enc_c)
        # return "".join(enc)
        bytes = "".join(enc).encode('utf-8')
        b64 = base64.urlsafe_b64encode(bytes)
        return b64.decode("utf-8")
        return "".join(enc).encode('utf-8').decode('cp1251')

        # return str(base64.urlsafe_b64encode("".join(enc)))

    @staticmethod
    def decode(key, enc):
        key = SimpleEncrypt.__get_key() + key
        dec = []
        enc_bytes = enc.encode("utf-8")
        bytes = base64.urlsafe_b64decode(enc_bytes)
        enc = bytes.decode('utf-8')
        # enc = base64.urlsafe_b64decode(bytes)
        for i in range(len(enc)):
            key_c = key[i % len(key)]
            dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
            dec.append(dec_c)
        return "".join(dec)

    @staticmethod
    def __get_key():
        import socket
        return socket.gethostname()


if __name__ == '__main__':
    import sys

    args = sys.argv
    if len(args) > 1:
        if len(args) != 4:
            print('usage: encrypt.py [encode|decode] "[key]" "[string]"')
        elif 'enc' in args[1]:
            print(SimpleEncrypt.encode(args[2], args[3]))
        elif 'dec' in args[1]:
            print(SimpleEncrypt.decode(args[2], args[3]))
        else:
            print('usage: encrypt.py [encode|decode] "[key]" "[string]"')

if __name__ == '__main__':
    s = SimpleEncrypt.encode('pwd', 'password')
    print(s)

    d = SimpleEncrypt.decode('key', s)
    print(d)


