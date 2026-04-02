#!/bin/bash
# 1. 重启数据库确保干净环境
../installed/bin/pg_ctl -D ~/data/pg_tpch -o "-p 55442" restart
sleep 2

# 2. 启动一个后台 psql 进程并获取它的 Backend PID
# 我们通过查看 ps 输出来精准定位刚启动的 backend
(echo "SELECT pg_sleep(60);" | ../installed/bin/psql -p 55442 -d tpch) &
PSQL_PID=$!
sleep 1

# 精准查找正在执行 pg_sleep 的 postgres 进程
BACKEND_PID=$(ps aux | grep "postgres: chenyunwen tpch" | grep "idle" -v | grep -v grep | awk '{print $2}')

if [ -z "$BACKEND_PID" ]; then
    echo "错误: 无法找到 Backend PID"
    kill $PSQL_PID
    exit 1
fi

echo "找到 Backend PID: $BACKEND_PID"

# 3. 准备 LLDB 脚本
cat <<EOF > /tmp/debug_script.lldb
process attach --pid $BACKEND_PID
continue
bt
quit
EOF

# 4. 在另一个进程中触发崩溃
# 我们先让 lldb 跑起来等待
echo "正在启动 LLDB 并附着..."
(lldb --batch -s /tmp/debug_script.lldb > /tmp/crash_bt.txt 2>&1) &
sleep 3

echo "正在触发崩溃查询..."
../installed/bin/psql -p 55442 -d tpch -c "LOAD 'llvmjit'; LOAD 'pg_volvec'; SET pg_volvec.enabled = on; SELECT sum(l_quantity) FROM lineitem;"

# 5. 等待并显示堆栈
sleep 2
echo "--- 崩溃堆栈输出 ---"
cat /tmp/crash_bt.txt

# 清理
rm /tmp/debug_script.lldb
kill $PSQL_PID 2>/dev/null
