import jieba
import sys
import os
import re
import jieba.posseg

def split_jieba(line):
    train = []
    seg_list = jieba.posseg.cut(line)
    ls = ""
    for w in seg_list:
        ls += w.word +':'+w.flag + ' '
    return ls
