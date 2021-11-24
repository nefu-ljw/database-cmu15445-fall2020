## cmu-db的安装

Database system project based on CMU 15-445/645 (FALL 2020) 

参考：https://github.com/cmu-db/bustub/blob/master/README.md


---

### 克隆存储库

github上新建一个仓库，名为`database-cmu15445-fall2020`。

```bash
git clone git@github.com:cmu-db/bustub.git ./database-cmu15445-fall2020
cd database-cmu15445-fall2020

$ git reset --hard 444765a
HEAD 现在位于 444765a Add PR template. (#156)

git branch -m master main #本地分支重命名
git remote rm origin #删除原仓库的远程分支
git remote add origin git@github.com:nefu-ljw/database-cmu15445-fall2020.git #添加自己仓库作为远程分支

#git remote add public git@github.com:cmu-db/bustub.git
#git pull public master

git push -u origin main
```

### build

经测试，不支持CentOS，支持ubuntu 18.04或20.04。

```bash
sudo build_support/packages.sh #自动安装BusTub需要的包（支持ubuntu 18.04或20.04）
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug .. #调试模式
make -j 8
```
