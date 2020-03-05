import React from 'react';
import { ReactComponent as GA } from '@svg/ga.svg';
import { ReactComponent as Amplitude } from '@svg/am.svg';
import { ReactComponent as Mixpanel } from '@svg/mp.svg';
import { ReactComponent as Facebook } from '@svg/fb.svg';
import { ReactComponent as Adjust } from '@svg/adj.svg';
import { ReactComponent as HubSpot } from '@svg/hs.svg';
import { ReactComponent as S3 } from '@svg/s3.svg';
import { ReactComponent as GCS } from '@svg/gcs.svg';
import { ReactComponent as Minio } from '@svg/minio.svg';
import { ReactComponent as Redshift } from '@svg/rs.svg';
import { ReactComponent as BigQuery } from '@svg/bq.svg';
import { ReactComponent as Snowflake } from '@svg/snowflake.svg';
import { ReactComponent as AppsFlyer } from '@svg/af.svg';
import { ReactComponent as Mailchimp } from '@svg/mc.svg';
import { ReactComponent as HotJar } from '@svg/hotjar.svg';
import { ReactComponent as Salesforce } from '@svg/salesforce.svg';
import { ReactComponent as Segment } from '@svg/segment.svg';
import { ReactComponent as Autopilot } from '@svg/autopilot.svg';
import { ReactComponent as GoogleAds } from '@svg/googleads.svg';
import { ReactComponent as AzureBlobStorage } from '@svg/azure-blob-storage.svg';
import { ReactComponent as VWO } from '@svg/vwo.svg';
import { ReactComponent as Intercom } from '@svg/intercom.svg';
import { ReactComponent as Heap } from '@svg/heap.svg';
import { ReactComponent as Branch } from '@svg/branch.svg';
import { ReactComponent as Kochava } from '@svg/kochava.svg';
import { ReactComponent as GTM } from '@svg/gtm.svg';
import { ReactComponent as Braze } from '@svg/braze.svg';
import { ReactComponent as KEEN } from '@svg/keen.svg';
import { ReactComponent as KissMetrics } from '@svg/kissmetrics.svg';
import { ReactComponent as CustomerIO } from '@svg/customerio.svg';
import { ReactComponent as Chartbeat } from '@svg/chartbeat.svg';
import { ReactComponent as Comscore } from '@svg/heap.svg';
import { ReactComponent as Firebase } from '@svg/firebase.svg';
import { ReactComponent as Leanplum } from '@svg/leanplum.svg';
import theme from '@css/theme';

const DestinationIcon = (props: any) => {
  // Making medium size default.
  let height = props.height || theme.iconSize.medium;
  let width = props.width || theme.iconSize.medium;

  switch (props.destination.toLowerCase()) {
    case 'am':
      return <Amplitude width={width} height={height} />;
    case 'ga':
      return <GA width={width} height={height} />;
    case 'mp':
      return <Mixpanel width={width} height={height} />;
    case 'fb':
      return <Facebook width={width} height={height} />;
    case 'adj':
      return <Adjust width={width} height={height} />;
    case 'hs':
      return <HubSpot width={width} height={height} />;
    case 's3':
      return <S3 width={width} height={height} />;
    case 'gcs':
      return <GCS width={width} height={height} />;
    case 'minio':
      return <Minio width={width} height={height} />;
    case 'rs':
      return <Redshift width={width} height={height} />;
    case 'bq':
      return <BigQuery width={width} height={height} />;
    case 'snowflake':
      return <Snowflake width={width} height={height} />;
    case 'af':
      return <AppsFlyer height={height} />;
    case 'mailchimp':
      return <Mailchimp width={width} height={height} />;
    case 'hotjar':
      return <HotJar width={width} height={height} />;
    case 'salesforce':
      return <Salesforce width={width} height={height} />;
    case 'segment':
      return <Segment width={width} height={height} />;
    case 'autopilot':
      return <Autopilot width={width} height={height} />;
    case 'googleads':
      return <GoogleAds width={width} height={height} />;
    case 'azure_blob':
      return <AzureBlobStorage width={width} height={height} />;
    case 'vwo':
      return <VWO width={width} height={height} />;
    case 'intercom':
      return <Intercom width={width} height={height} />;
    case 'heap':
      return <Heap width={width} height={height} />;
    case 'branch':
      return <Branch width={width} height={height} />;
    case 'gtm':
      return <GTM width={width} height={height} />;
    case 'braze':
      return <Braze width={width} height={height} />;
    case 'keen':
      return <KEEN width={width} height={height} />;
    case 'kochava':
      return <Kochava width={width} height={height} />;
    case 'kissmetrics':
      return <KissMetrics width={width} height={height} />;
    case 'customerio':
      return <CustomerIO width={width} height={height} />;
    case 'chartbeat':
      return <Chartbeat width={width} height={height} />;
    case 'comscore':
      return <Comscore width={width} height={height} />;
    case 'firebase':
      return <Firebase width={width} height={height} />;
    case 'leanplum':
      return <Leanplum width={width} height={height} />;
    default:
      break;
  }
  return <div />;
};

export default DestinationIcon;
