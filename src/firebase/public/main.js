/**
 * Copyright 2017 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

function Demo() {
  $(function() {
    this.$signInButton = $('#demo-sign-in-button');
    this.$signOutButton = $('#demo-sign-out-button');
    this.$queryTextarea = $('#demo-query');
    this.$runQueryButton = $('#demo-run-query');
    this.$runQueryResult = $('#demo-run-query-result');

    this.$signInButton.on('click', this.signIn.bind(this));
    this.$signOutButton.on('click', this.signOut.bind(this));
    this.$runQueryButton.on('click', this.runQuery.bind(this));

    firebase.auth().onAuthStateChanged(this.onAuthStateChanged.bind(this));
  }.bind(this));
}

Demo.prototype.signIn = function() {
  firebase.auth().signInWithPopup(new firebase.auth.GoogleAuthProvider());
};

Demo.prototype.signOut = function() {
  firebase.auth().signOut();
};

Demo.prototype.onAuthStateChanged = function(user) {

  // Ref: https://medium.com/swlh/hasura-authentication-with-firebase-ee5543d57772
  // get the refresh token updated with Hasura claims
  if (user) {
    return user.getIdToken().then((token) => firebase.auth().currentUser.getIdTokenResult()
      .then((result) => {
        if (result.claims['https://hasura.io/jwt/claims']) {
          return token
        }
        // DEV (emulator)
        // const endpoint = 'http://localhost:5001/gainy-dev/us-central1/refreshToken'

        // PROD
        const endpoint = 'https://us-central1-gainy-dev.cloudfunctions.net/refreshToken'
        return fetch(`${endpoint}?uid=${user.uid}`).then((res) => {
          if (res.status === 200) {
            return user.getIdToken(true)
          }
          return res.json().then((e) => { throw e })
        })
      })).then((validToken) => {
      // Store Token / Or create Apollo with your new token!
    }).catch(console.error)
  }
};

Demo.prototype.runQuery = function() {
  var query = this.$queryTextarea.val();

  if (query === '') return;

  // local dev hasura API endpoint
  var hasura = 'http://localhost:8080/v1/graphql';

  // Make an authenticated POST request to run a graphql query
  this.authenticatedRequest('POST', hasura, {query: query}).then(function(response) {
    // this.$queryTextarea.val('');
    this.$queryTextarea.parent().removeClass('is-dirty');

    if (response.errors) {
      this.$runQueryResult.html('Error: <b>' + JSON.stringify(response.errors[0], null, 2) + '</b>');
    } else {
      this.$runQueryResult.html('Result: <b>' + JSON.stringify(response.data, null, 2) + '</b>');
    }
    console.log('Response:', response);

  }.bind(this)).catch(function(error) {
    console.log('Error running query:', query);
    throw error;
  });
};

Demo.prototype.authenticatedRequest = function(method, url, body) {
  if (!firebase.auth().currentUser) {
    throw new Error('Not authenticated. Make sure you\'re signed in!');
  }

  // Get the Firebase auth token to authenticate the request
  return firebase.auth().currentUser.getIdToken().then(function(token) {
    var request = {
      method: method,
      url: url,
      dataType: 'json',
      beforeSend: function(xhr) { xhr.setRequestHeader('Authorization', 'Bearer ' + token); }
    };

    if (method === 'POST') {
      request.contentType = 'application/json';
      request.data = JSON.stringify(body);
    }

    console.log('Making authenticated request:', method, url);
    return $.ajax(request).catch(function() {
      throw new Error('Request error: ' + method + ' ' + url);
    });
  });
};

window.demo = new Demo();
