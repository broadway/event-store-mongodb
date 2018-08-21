FROM php:7.2-cli

RUN apt-get update -yqq
RUN apt-get install -y wget zip -yqq

# Install composer
RUN wget https://composer.github.io/installer.sig -O - -q | tr -d '\n' > installer.sig \
    && php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');" \
    && php -r "if (hash_file('SHA384', 'composer-setup.php') === file_get_contents('installer.sig')) { echo 'Installer verified'; } else { echo 'Installer corrupt'; unlink('composer-setup.php'); } echo PHP_EOL;" \
    && php composer-setup.php \
    && php -r "unlink('composer-setup.php'); unlink('installer.sig');" \
    && chmod +x composer.phar \
    && mv composer.phar /usr/local/bin/composer

# Install required php extensions
RUN pecl install mongodb \
    && docker-php-ext-enable mongodb
