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
  GA: raw('./GA.md'),
  AM: raw('./AM.md'),
  MP: raw('./MP.md'),
  FB: raw('./FB.md'),
  ADJ: raw('./ADJ.md'),
  HS: raw('./HS.md'),
  S3: raw('./S3.md'),
  MINIO: raw('./MINIO.md'),
  RS: raw('./RS.md'),
  BQ: raw('./BQ.md'),
  SNOWFLAKE: raw('./SNOWFLAKE.md'),
  AF: raw('./AF.md'),
  MAILCHIMP: raw('./MAILCHIMP.md'),
  HOTJAR: raw('./HOTJAR.md'),
  SALESFORCE: raw('./SALESFORCE.md'),
  SEGMENT: raw('./SEGMENT.md'),
  AUTOPILOT: raw('./AUTOPILOT.md'),
  GOOGLEADS: raw('./GOOGLEADS.md'),
  VWO: raw('./VWO.md'),
  INTERCOM: raw('./INTERCOM.md'),
  BRANCH: raw('./BRANCH.md'),
  BRAZE: raw('./BRAZE.md'),
  GTM: raw('./GTM.md'),
  HEAP: raw('./HEAP.md'),
  KEEN: raw('./KEEN.md'),
  KOCHAVA: raw('./KOCHAVA.md'),
  KISSMETRICS: raw('./KISSMETRICS.md'),
  CUSTOMERIO: raw('./CUSTOMERIO.md'),
  CHARTBEAT: raw('./CHARTBEAT.md'),
  COMSCORE: raw('./COMSCORE.md'),
  FIREBASE: raw('./FIREBASE.md'),
  LEANPLUM: raw('./LEANPLUM.md'),
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
