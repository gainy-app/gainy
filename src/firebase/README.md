# Firebase auth for Hasura

Gainy mobile app will use Google Firebase SDK to let users login with their existing 3rd party account (Google, Facebook, etc).

## Local set-up
1. Create the firebase project (Step 2 below, substeps 1-5);
2. Obtain the key json file and save it as `firebase.key.json`;
3. `cp .env{.dist,}` && `cp public/index-local.html{.dist,}` && `cp public/main-local.js{.dist,}`. Modify endpoint variable in `public/main-local.js` and `HASURA_GRAPHQL_JWT_SECRET` env variable in hasura setup;
4. Authorize firebase:
```bash
docker-compose up -d
docker-compose exec -T node firebase login:ci
# you'll see the firebase token - copy-paste it to the .env file
```
5. Restart containers and run emulators: `docker-compose exec -T node firebase emulators:start --only functions,hosting` 
6. Access demo at `http://localhost:8081/`.

With this configuration you can authenticate as `user` role in hasura with Google Auth.
TODO:
1. set up hasura to accept queries to public collections without Authorization header;
2. set up hasura to respond with sane code and body when permission is denied.

## Our (preliminary) security model:
    - all users authenticate to the mobile app with their existing Google account ("Sign-in with Google")
    - all users assume the same restricted (read-only) role in Hasura
    - anonymous access is permitted with the "anonymous" role
    - regular users authenticate with a JWT Bearer token
    - admin access is granted by checking `X-Hasura-Admin-Secret` in the HTTPS headers, which overrides and disables JWT auth
    - transport security (`https://`) is required, we need a valid SSL certificate the the domain hosting Hasura


## Firebase-Hasura security specifics

    1. We create a project in Firebase and become a Firebase tenant
    2. Firebase will authenticate the users and issue JWTs
    3. JWTs will be signed by Firebase’s JWKs
    4. Hasura will verify the JWT by fetching the pre-configured JWK from Firebase and validating the signature (the JWK could be configured static, but Firebase rotates the keys, so has to be configured as a URL).
    5. Firebase shares JWKs between its tenants, so an attack could exist where an attacker creates their project in Firebase, authenticates and gets a valid JWT signed by a shared JWK and is able to login to our Hasura
    6. To prevent this, Hasura must be configured to validate the aud (audience) claim as part of JWT verification, which will match the Firebase project ID of our project


## Step 1 - configure Hasura

    1. Create user roles in Hasura - read/only, admin etc. and set the desired access permissions accordingly.
       For the PoC I have created a read-only role called "user".
    2. Generate JWT config for Hasura:
       - go to [https://hasura.io/jwt-config/](https://hasura.io/jwt-config/)
       - select Firebase as the provider and enter the name of the Firebase project ("gainy-dev").
    3. Use env HASURA_GRAPHQL_JWT_SECRET in the container to specify the JWT config generated in step 2. See src/hasura/docker-compose.yml
    4. Setting this env puts Hasura in "JWT mode"
        NOTE: In JWT mode, on a secured endpoint:
        * JWT authentication is enforced when the X-Hasura-Admin-Secret header is not found in the request.
        * JWT authentication is skipped when the X-Hasura-Admin-Secret header is found in the request and admin access is granted.


## Step 2 - configure Firebase

    Firebase needs to be configured to issue tokens for Hasura; this includes adding the custom namespace and X-Hasura specific claims under this namespace in the JWT.

    1. The JWT must contain: x-hasura-default-role, x-hasura-allowed-roles in a custom namespace in the claims.
    2. Other optional x-hasura-* fields (required as per our defined permissions).
    3. Send the JWT via Authorization: Bearer <JWT> header.

    We use Firebase cloud functions to customize the JWT with Hasura namespace and claims. A cloud function automatically runs in response to events triggered by Firebase features and HTTPS requests. In our case the events are firebase user creation and token refresh.

    1. Create a new project in Firebase (I used "gainy-dev" for this PoC)
    2. Go to configure Authentication
    3. In Sign-in providers enable Google
    4. (prod, future) in Authorized domains add any custom domains for the mobile/web app if needed
    5. Upgrade from the Free plan to Pay as you go to enable Cloud Functions
    6. Install Firebase tools. Note: firebase wants node v14.
```bash
        $ npm install -g firebase-tools
        $ firebase login
        $ firebase init
```
    7. Enable Functions
    8. Set up emulators for local dev
        `$ firebase init emulators`
       Enable Authentication, Functions, Hosting.

    9. Set up the active project
        `$ firebase use gainy-dev`

    10. Edit functions/index.js to implement our cloud functions.
        To test locally in emulator
        `$ firebase emulators:start --only functions,hosting`

        To deploy to Firebase
        `$ firebase deploy --only functions`

        (!) When deployed live (or in emulator) firebase will display a URI for token refresh. This URI will then need to be configured into the mobile app. See an example of this in `main.js`:
```javascript
        // DEV (emulator)
        // const endpoint = 'http://localhost:5001/gainy-dev/us-central1/refreshToken'
        // PROD
        // const endpoint = 'https://us-central1-gainy-dev.cloudfunctions.net/refreshToken'
```


## Step 3 - App integration

    I hacked together a Web app under `/public` to test and demo the workflow.
    `main.js` contains function calls to Firebase JS SDK to handle auth. The app will authenticate to Firebase and receive a JWT bearer token which it will then use to send an authenticated POST request to Hasura.

    The app can be run locally using the firebase emulator that starts a localhost web server.
    `$ firebase emulators:start --only hosting`

    Note inside `index.html` we can configure the SDK to use emulators or the real cloud environment by tweaking `<script src="/__/firebase/init.js?useEmulator=false"></script>`.
