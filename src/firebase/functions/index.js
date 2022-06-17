// Ref: https://medium.com/swlh/hasura-authentication-with-firebase-ee5543d57772

const functions = require('firebase-functions')
const admin = require('firebase-admin')
const cors = require('cors')({ origin: true })

admin.initializeApp(functions.config().firebase)

const updateClaims = (uid, isAnonymous) => {
  const role = isAnonymous ? 'anonymous' : 'user';

  admin.auth().setCustomUserClaims(uid, {
    'https://hasura.io/jwt/claims': {
      'x-hasura-default-role': role,
      'x-hasura-allowed-roles': [role],
      'x-hasura-user-id': uid,
    },
  });
}

exports.processSignUp = functions.auth.user().onCreate((user) => {
  console.log('ON CREATE', user)
  updateClaims(user.uid, user.sign_in_provider);
})

exports.refreshToken = functions.https.onRequest((req, res) => {
  console.log('TOKEN REFRESH', req.query)
  cors(req, res, () => {
    updateClaims(req.query.uid, req.query.isAnonymous).then(() => {
      res.status(200).send('success')
    }).catch((error) => {
      console.error('REFRESH ERROR', error)
      res.status(400).send(error)
    })
  })
})
