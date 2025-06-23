import React, { useState } from 'react';
import { auth, createUserWithEmailAndPassword, signInWithEmailAndPassword, signOut, GoogleAuthProvider, signInWithPopup } from '../services/firebase';

const AuthForm = () => {
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');

    const handleSignup = async () => {
        try {
            await createUserWithEmailAndPassword(auth, email, password);
            console.log("Signup successful");
        } catch (error) {
            console.error("Signup error:", error);
        }
    };

    const handleLogin = async () => {
        try {
            await signInWithEmailAndPassword(auth, email, password);
            const user = auth.currentUser;
            console.log("Login successful");
            console.log(user);
        } catch (error) {
            console.error("Login error:", error);
        }
    };

    const handleGoogleSignup = async () => {
        try {
            // Google Auth Provider
            const provider = new GoogleAuthProvider();
            await signInWithPopup(auth, provider);
            console.log("Google Signup successful");
        } catch (error) {
            console.error("Google Signup error:", error);
        }
    }

    const handleGoogleLogin = async () => {
        try {
            // Google Auth Provider
            const provider = new GoogleAuthProvider();
            await signInWithPopup(auth, provider);
            console.log("Google Login successful");
        } catch (error) {
            console.error("Google Login error:", error);
        }
    }

    const handleLogout = async () => {
        try {
            await signOut(auth);
            console.log("Logout successful");
        } catch (error) {
            console.error("Logout error:", error);
        }
    };

    return (
        <div>
            <input type="text" value={email} onChange={e => setEmail(e.target.value)} placeholder="Email" />
            <input type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="Password" />
            <button onClick={handleSignup}>Signup</button>
            <button onClick={handleLogin}>Login</button>
            <button onClick={handleLogout}>Logout</button>
            {/* sso singin and singup google */}
            <button onClick={handleGoogleSignup}>Google Signup</button>
            <button onClick={handleGoogleLogin}>Google Login</button>

        </div>
    );
};

export default AuthForm;
