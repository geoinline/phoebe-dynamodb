/**
 * 
 */
package com.swengle.phoebe.auth;

import java.util.Date;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class STSSessionCredentialsProviderWithClientConfiguration implements AWSCredentialsProvider {
    /** Default duration for started sessions */
    public static final int DEFAULT_DURATION_SECONDS = 3600;

    /** The client for starting STS sessions */
    private final AWSSecurityTokenService securityTokenService;

    /** The current session credentials */
    private AWSSessionCredentials sessionCredentials;

    /** The expiration time for the current session credentials */
    private Date sessionCredentialsExpiration;
	
	/**
	 * 
	 */
	public STSSessionCredentialsProviderWithClientConfiguration(AWSCredentials longLivedCredentials, ClientConfiguration clientConfiguration) {
		securityTokenService = new AWSSecurityTokenServiceClient(longLivedCredentials, clientConfiguration);
	}
	
	public STSSessionCredentialsProviderWithClientConfiguration(AWSCredentialsProvider longLivedCredentialsProvider, ClientConfiguration clientConfiguration) {
		securityTokenService = new AWSSecurityTokenServiceClient(longLivedCredentialsProvider, clientConfiguration);
	}

    @Override
    public AWSCredentials getCredentials() {
        if (needsNewSession()) startSession();

        return sessionCredentials;
    }

    @Override
    public void refresh() {
        startSession();
    }

    /**
     * Starts a new session by sending a request to the AWS Security Token
     * Service (STS) with the long lived AWS credentials. This class then vends
     * the short lived session credentials sent back from STS.
     */
    private void startSession() {
        GetSessionTokenResult sessionTokenResult = securityTokenService
                .getSessionToken(new GetSessionTokenRequest().withDurationSeconds(DEFAULT_DURATION_SECONDS));
        Credentials stsCredentials = sessionTokenResult.getCredentials();

        sessionCredentials = new BasicSessionCredentials(
                stsCredentials.getAccessKeyId(),
                stsCredentials.getSecretAccessKey(),
                stsCredentials.getSessionToken());
        sessionCredentialsExpiration = stsCredentials.getExpiration();
    }

    /**
     * Returns true if a new STS session needs to be started. A new STS session
     * is needed when no session has been started yet, or if the last session is
     * within 60 seconds of expiring.
     *
     * @return True if a new STS session needs to be started.
     */
    private boolean needsNewSession() {
        if (sessionCredentials == null) return true;

        long timeRemaining = sessionCredentialsExpiration.getTime() - System.currentTimeMillis();
        return timeRemaining < (60 * 1000);
    }

}
