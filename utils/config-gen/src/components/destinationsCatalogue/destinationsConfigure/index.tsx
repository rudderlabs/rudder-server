import * as React from 'react';
import { IDestinationDef } from '@stores/destinationDefsList';
import ReactMarkdown from 'react-markdown';

// @ts-ignore
import raw from 'raw.macro';
import { ButtonPrimary } from '@components/common/button';
import { Flex } from '../../common/misc';
import DestinationIcon from '@components/icons/destinationIcon';
import { Header, TextDiv } from '../../common/typography';
import theme from '@css/theme';
import { Link } from 'react-router-dom';

const markdown = {
  GA: raw('./v1/GOOGLE_ANALYTICS.md'), 
  AM: raw('./v1/AMPLITUDE.md'), 
  MP: raw('./v1/MIXPANEL.md'), 
  FB: raw('./FB.md'),
  ADJ: raw('./v1/ADJUST.md'), 
  HS: raw('./v1/HUBSPOT.md'), 
  S3: raw('./v1/AMAZON_S3.md'), 
  MINIO: raw('./v1/MINIO.md'), 
  RS: raw('./v1/AMAZON_REDSHIFT.md'), 
  BQ: raw('./v1/GOOGLE_BIGQUERY.md'), 
  SNOWFLAKE: raw('./v1/SNOWFLAKE.md'), 
  AF: raw('./v1/APPSFLYER.md'), 
  MAILCHIMP: raw('./v1/MAILCHIMP.md'), 
  HOTJAR: raw('./v1/HOTJAR.md'), 
  SALESFORCE: raw('./v1/SALESFORCE.md'), 
  SEGMENT: raw('./SEGMENT.md'),
  AUTOPILOT: raw('./AUTOPILOT.md'),
  GOOGLEADS: raw('./v1/GOOGLE_ADS.md'), 
  VWO: raw('./v1/VWO.md'), 
  INTERCOM: raw('./v1/INTERCOM.md'), 
  BRANCH: raw('./v1/BRANCH.md'), 
  BRAZE: raw('./v1/BRAZE.md'), 
  GTM: raw('./v1/GOOGLE_TAG_MANAGER.md'), 
  HEAP: raw('./HEAP.md'),
  KEEN: raw('./v1/KEEN_IO.md'), 
  KOCHAVA: raw('./v1/KOCHAVA.md'), 
  KISSMETRICS: raw('./v1/KISSMETRICS.md'), 
  CUSTOMERIO: raw('./v1/CUSTOMER_IO.md'), 
  CHARTBEAT: raw('./v1/CHARTBEAT.md'), 
  COMSCORE: raw('./COMSCORE.md'),
  FIREBASE: raw('./v1/FIREBASE.md'), 
  LEANPLUM: raw('./v1/LEANPLUM.md'), 
  WEBHOOK: raw('./WEBHOOK.md'),
  //PERSONALIZE: raw('./PERSONALIZE.md'),
  FB_PIXEL: raw('./FB_PIXEL.md'),
  // PERSONALIZE: raw('./PERSONALIZE.md'),
  LOTAME: raw('./LOTAME.md'),
  SLACK: raw('./SLACK.md'),
  ZENDESK: raw('./ZENDESK.md'),
  AZURE_BLOB: raw('./v1/MICROSOFT_AZURE_BLOB_STORAGE.md'), 
  FULLSTORY: raw('./v1/FULLSTORY.md'), 
  GCS: raw('./v1/GOOGLE_CLOUD_STORAGE.md'), 
  BUGSNAG: raw('./BUGSNAG.md'),
  KINESIS: raw('./KINESIS.md'),
  KAFKA: raw('./KAFKA.md'),
  AZURE_EVENT_HUB: raw('./AZURE_EVENT_HUB.md'),
  ITERABLE: raw('./ITERABLE.md')
};

export interface IDestinationConfigureProps {
  destinationDef?: IDestinationDef;
}
export interface IDestinationConfigureState {
  markdown: string;
}

export default class DestinationConfigure extends React.Component<
  IDestinationConfigureProps,
  IDestinationConfigureState
> {
  constructor(props: IDestinationConfigureProps) {
    super(props);
    this.state = {
      markdown: '',
    };
  }

  public render() {
    const { destinationDef } = this.props;
    return (
      <div className="p-l-lg">
        <Flex className="m-b-lg p-b-md b-b-grey" alignItems="center">
          <DestinationIcon
            destination={destinationDef!.name}
            height={theme.iconSize.large}
            width={theme.iconSize.large}
          ></DestinationIcon>
          <div className="m-l-md">
            <Header color={theme.color.black}>
              {destinationDef!.displayName}
            </Header>
          </div>
        </Flex>
        <div>
          <Link
            to={`/destinations/setup?destinationDefId=${destinationDef!.id}`}
          >
            <ButtonPrimary className="m-b-lg">Configure</ButtonPrimary>
          </Link>
          <ReactMarkdown source={(markdown as any)[destinationDef!.name]} />
        </div>
      </div>
    );
  }
}
