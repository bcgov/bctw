FROM node:18

USER 0

ENV HOME=/opt/app-root/src

# Create the app directory
RUN mkdir -p $HOME

# Set permissions for the app directory
RUN chgrp -R 0 $HOME && \
    chmod -R g+rwX $HOME

WORKDIR $HOME

# Optional: Remove build directory if it exists
RUN rm -rf ./build

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN npm ci

# Copy the rest of the application code
COPY . ./

# Set PATH for local node_modules
ENV PATH ${HOME}/node_modules/.bin/:${HOME}/.npm-global/bin/:${HOME}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Switch to non-root user
USER 1001

# Run build process
RUN npm run build

# Execute the scripts
CMD [ "node", "src/index.js" ]
