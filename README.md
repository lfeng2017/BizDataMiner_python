# BizDataMiner_python
该项目中可能用到的知识包括：

axis=0表述列 
axis=1表述行

Python常用函数：http://blog.csdn.net/bravezhe/article/details/7653446

NumPy入门学习：http://old.sebug.net/paper/books/scipydoc/numpy_intro.html


Pandas入门学习：http://www.open-open.com/lib/view/open1402477162868.html
http://pda.readthedocs.io/en/latest/chp5.html
http://www.cnblogs.com/kylinlin/p/5226790.html（重点看这篇）
dataframe筛选数据：http://jingyan.baidu.com/article/0eb457e508b6d303f0a90572.html
分组：http://www.aichengxu.com/python/16058.htm



Azkaban:入门学习：http://azkaban.github.io/azkaban/docs/latest/

SQLAlchemy入门学习：http://www.jb51.net/article/49789.htm
http://www.cnblogs.com/zhangju/p/5720210.html
http://blog.csdn.net/linda1000/article/details/8040190

fabric入门学习：http://www.cnblogs.com/aslongas/p/5961144.html

Python命令行神器 Click ：http://blog.csdn.net/lihua_tan/article/details/54869355?utm_source=itdadao&utm_medium=referral



map()函数
map()是 Python 内置的高阶函数，它接收一个函数 f 和一个 list，并通过把函数 f 依次作用在 list 的每个元素上，得到一个新的 list 并返回。
def f(x):
    return x*x
print map(f, [1, 2, 3, 4, 5, 6, 7, 8, 9])
输出结果：
[1, 4, 9, 10, 25, 36, 49, 64, 81]
