# Quant Auto Backtesting Analysis Trade Framework
> ———— 自动回测分析交易框架

# 安装
系统环境要求：
>Ubuntu 16.04 \
Python 3.5 \
MySQL 5.7 \
Redis 3.0.6

Python库要求：
>pyctp https://github.com/mmmaaaggg/pyctp_lovelylain
环境编译
win10、python 3.5
需要安装 visualcppbuildtools_full.exe 或 vc2015 完成编译

windows 环境下编译可能出现各种问题

例如：

1. cl.exe文件找不到

百度查方法吧，一大堆结果

2. LINK : fatal error LNK1158: 无法运行“rc.exe”

error: command 'C:\\Program Files (x86)\\Microsoft Visual Studio 14.0\\VC\\BIN\\x86_amd64\\link.exe' failed with exit status 1158
拷贝：
C:\Program Files (x86)\Windows Kits\10\bin\10.0.18362.0\x64
下面的 rc.exe, rcdll.dll 两个文件 到
C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\bin\x86_amd64
目录下


# 运行
> config.py 配置文件 \
md_broadcast.py 行情广播文件 \
task.py 后台salary任务执行程序
