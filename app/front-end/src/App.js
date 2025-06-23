import React from 'react';
import { auth } from './services/firebase';

import AuthForm from './components/AuthForm';

function App() {
  const [user, setUser] = React.useState(null);

  window.onload = function () {
    console.log('window loaded');
    auth.onAuthStateChanged(user => {
      setUser(user);
    }
    );
  }

  if (user) {
    console.log('User is signed in:', user);
    user.getIdToken(true)
      .then((idToken) => {
        // Faça a solicitação para a API protegida usando o ID Token
        return fetch('http://localhost:8000/api/v1/protected', {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${idToken}`, // Use o ID Token aqui
          },
        });
      })
      .then(response => response.json())
      .then(data => {
        console.log('API response:', data);
      })
      .catch(error => {
        console.error('API error:', error);
      });
  }
  return (
    <div className="App">
      <h1>Authentication</h1>
      <AuthForm />

      {auth.currentUser && (
        <div>
          <h2>User Information</h2>
          <ul>
            <li>Email: {auth.currentUser.email}</li>
            <li>UID: {auth.currentUser.uid}</li>
            <li>Token: {auth.currentUser.za}</li>
            <li>Refresh Token: {auth.currentUser.refreshToken}</li>
            <li>Provider: {auth.currentUser.providerData[0].providerId}</li>
            <li>Creation Time: {auth.currentUser.metadata.creationTime}</li>
            <li>Last Sign In Time: {auth.currentUser.metadata.lastSignInTime}</li>
            <li>Photo URL: {auth.currentUser.photoURL}</li>
            <li>Phone Number: {auth.currentUser.phoneNumber}</li>
            <li>Is Anonymous: {auth.currentUser.isAnonymous.toString()}</li>
            <li>Email Verified: {auth.currentUser.emailVerified.toString()}</li>
          </ul>
        </div>
      )}
    </div>
  );
}

export default App;
