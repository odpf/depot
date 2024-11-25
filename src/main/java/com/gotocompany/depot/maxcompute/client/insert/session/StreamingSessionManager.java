package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

public interface StreamingSessionManager {
    TableTunnel.StreamUploadSession getSession(String sessionId) throws TunnelException;
    void clearSession(String sessionId);
}
