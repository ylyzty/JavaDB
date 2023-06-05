# Java DB

**XID文件**

事务 -> XID(事务ID，1开始自增，不可重复)
XID(0) -> 超级事务, 该事务的状态永远为 commited

`TransactionManager`: 维护 XID 文件，记录各个事务的状态(占用1个字节)，文件头部使用8字节保存事务数量

- ACTIVE: 正在进行
- COMMITED: 已提交
- ABORTED: 回滚



**Git**

```shell
# Create a new repo
git init
git branch -M main
git remote add origin ""
git push -u origin main
```

```shell
# Push existing repo
git remote add origin ""
git branch -M main
git push -u origin main
```







