package client

import (
	"gopkg.in/jcmturner/gokrb5.v6/iana/flags"
	"gopkg.in/jcmturner/gokrb5.v6/iana/nametype"
	"gopkg.in/jcmturner/gokrb5.v6/krberror"
	"gopkg.in/jcmturner/gokrb5.v6/messages"
	"gopkg.in/jcmturner/gokrb5.v6/types"
)

// TGSExchange performs a TGS exchange to retrieve a ticket to the specified SPN.
// The ticket retrieved is added to the client's cache.
func (cl *Client) TGSExchange(spn types.PrincipalName, kdcRealm string, tgt messages.Ticket, sessionKey types.EncryptionKey, renewal bool, referral int) (tgsReq messages.TGSReq, tgsRep messages.TGSRep, err error) {
	tgsReq, err = messages.NewTGSReq(cl.Credentials.CName, kdcRealm, cl.Config, tgt, sessionKey, spn, renewal)
	if err != nil {
		return tgsReq, tgsRep, krberror.Errorf(err, krberror.KRBMsgError, "TGS Exchange Error: failed to generate a new TGS_REQ")
	}
	tgsRep, err = cl.tgsExchange(tgsReq, kdcRealm, sessionKey)
	if err != nil {
		return
	}
	// TODO should this check the first element is krbtgt rather than the nametype?
	if tgsRep.Ticket.SName.NameType == nametype.KRB_NT_SRV_INST && !tgsRep.Ticket.SName.Equal(spn) {
		if referral > 5 {
			return tgsReq, tgsRep, krberror.Errorf(err, krberror.KRBMsgError, "maximum number of referrals exceeded")
		}
		// Server referral https://tools.ietf.org/html/rfc6806.html#section-8
		// The TGS Rep contains a TGT for another domain as the service resides in that domain.
		cl.AddSession(tgsRep.Ticket, tgsRep.DecryptedEncPart)
		realm := tgsRep.Ticket.SName.NameString[len(tgsRep.Ticket.SName.NameString)-1]
		referral++
		return cl.TGSExchange(spn, realm, tgsRep.Ticket, tgsRep.DecryptedEncPart.Key, false, referral)
	}
	return
}

// TGSREQ exchanges the provides TGS_REQ with the KDC to retrieve a TGS_REP
func (cl *Client) TGSREQ(tgsReq messages.TGSReq, kdcRealm string, tgt messages.Ticket, sessionKey types.EncryptionKey, referral int) (messages.TGSReq, messages.TGSRep, error) {
	tgsRep, err := cl.tgsExchange(tgsReq, kdcRealm, sessionKey)
	if err != nil {
		return tgsReq, tgsRep, err
	}
	// TODO should this check the first element is krbtgt rather than the nametype?
	if tgsRep.Ticket.SName.NameType == nametype.KRB_NT_SRV_INST && !tgsRep.Ticket.SName.Equal(tgsReq.ReqBody.SName) {
		if referral > 5 {
			return tgsReq, tgsRep, krberror.Errorf(err, krberror.KRBMsgError, "maximum number of referrals exceeded")
		}
		// Server referral https://tools.ietf.org/html/rfc6806.html#section-8
		// The TGS Rep contains a TGT for another domain as the service resides in that domain.
		cl.AddSession(tgsRep.Ticket, tgsRep.DecryptedEncPart)
		realm := tgsRep.Ticket.SName.NameString[len(tgsRep.Ticket.SName.NameString)-1]
		referral++
		if types.IsFlagSet(&tgsReq.ReqBody.KDCOptions, flags.EncTktInSkey) && len(tgsReq.ReqBody.AdditionalTickets) > 0 {
			tgsReq, err = messages.NewUser2UserTGSReq(cl.Credentials.CName, kdcRealm, cl.Config, tgt, sessionKey, tgsReq.ReqBody.SName, tgsReq.Renewal, tgsReq.ReqBody.AdditionalTickets[0])
			if err != nil {
				return tgsReq, tgsRep, err
			}
		}
		tgsReq, err = messages.NewTGSReq(cl.Credentials.CName, kdcRealm, cl.Config, tgt, sessionKey, tgsReq.ReqBody.SName, tgsReq.Renewal)
		if err != nil {
			return tgsReq, tgsRep, err
		}
		return cl.TGSREQ(tgsReq, realm, tgsRep.Ticket, tgsRep.DecryptedEncPart.Key, referral)
	}
	return tgsReq, tgsRep, err
}

func (cl *Client) tgsExchange(tgsReq messages.TGSReq, kdcRealm string, sessionKey types.EncryptionKey) (tgsRep messages.TGSRep, err error) {
	b, err := tgsReq.Marshal()
	if err != nil {
		return tgsRep, krberror.Errorf(err, krberror.EncodingError, "TGS Exchange Error: failed to generate a new TGS_REQ")
	}
	r, err := cl.sendToKDC(b, kdcRealm)
	if err != nil {
		if _, ok := err.(messages.KRBError); ok {
			return tgsRep, krberror.Errorf(err, krberror.KDCError, "TGS Exchange Error: kerberos error response from KDC")
		}
		return tgsRep, krberror.Errorf(err, krberror.NetworkingError, "TGS Exchange Error: issue sending TGS_REQ to KDC")
	}
	err = tgsRep.Unmarshal(r)
	if err != nil {
		return tgsRep, krberror.Errorf(err, krberror.EncodingError, "TGS Exchange Error: failed to process the TGS_REP")
	}
	err = tgsRep.DecryptEncPart(sessionKey)
	if err != nil {
		return tgsRep, krberror.Errorf(err, krberror.EncodingError, "TGS Exchange Error: failed to process the TGS_REP")
	}
	if ok, err := tgsRep.IsValid(cl.Config, tgsReq); !ok {
		return tgsRep, krberror.Errorf(err, krberror.EncodingError, "TGS Exchange Error: TGS_REP is not valid")
	}
	return
}

// GetServiceTicket makes a request to get a service ticket for the SPN specified
// SPN format: <SERVICE>/<FQDN> Eg. HTTP/www.example.com
// The ticket will be added to the client's ticket cache
func (cl *Client) GetServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error) {
	var tkt messages.Ticket
	var skey types.EncryptionKey
	if tkt, skey, ok := cl.GetCachedTicket(spn); ok {
		// Already a valid ticket in the cache
		return tkt, skey, nil
	}
	princ := types.NewPrincipalName(nametype.KRB_NT_PRINCIPAL, spn)
	realm := cl.Config.ResolveRealm(princ.NameString[len(princ.NameString)-1])

	tgt, skey, err := cl.sessionTGT(realm)
	if err != nil {
		return tkt, skey, err
	}

	_, tgsRep, err := cl.TGSExchange(princ, realm, tgt, skey, false, 0)
	if err != nil {
		return tkt, skey, err
	}
	cl.cache.addEntry(
		tgsRep.Ticket,
		tgsRep.DecryptedEncPart.AuthTime,
		tgsRep.DecryptedEncPart.StartTime,
		tgsRep.DecryptedEncPart.EndTime,
		tgsRep.DecryptedEncPart.RenewTill,
		tgsRep.DecryptedEncPart.Key,
	)
	return tgsRep.Ticket, tgsRep.DecryptedEncPart.Key, nil
}
