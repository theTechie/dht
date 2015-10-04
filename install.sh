command -v python >/dev/null 2>&1 || { 
    echo >&2 "Python not installed. Installing Python...";
    sudo apt-get install software-properties-common python-software-properties
    sudo add-apt-repository ppa:fkrull/deadsnakes
    sudo apt-get update
    sudo apt-get install --yes python2.7
    ln -s /usr/bin/python2.7 /usr/bin/python
}
command -v node >/dev/null 2>&1 || { 
    echo >&2 "Node.js not installed. Installing Node.js...";
    sudo apt-get install --yes build-essential
    sudo apt-get install --yes curl
    curl -sL https://deb.nodesource.com/setup_0.12 | sudo bash -
    sudo apt-get install --yes nodejs
}
npm install
