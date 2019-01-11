#在工作中经常要为同事开通SVN权限，现在特写一个小脚本，以随机生成包含数字、特殊字符和英文字符的密码串
import string
import random

KEY_LEN = 10

def base_str():
    return (string.ascii_letters+string.digits+string.punctuation)
def key_gen():
    keylist = [random.choice(base_str()) for i in range(KEY_LEN)]
    return ("".join(keylist))

print(key_gen())
