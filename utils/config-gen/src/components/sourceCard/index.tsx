import { Flex } from '@components/common/misc';
import { LabelDiv, Text } from '@components/common/typography';
import theme from '@css/theme';
import { ISourceStore } from '@stores/source';
import { ISourceDefinitionsListStore } from '@stores/sourceDefinitionsList';
import { ReactComponent as Check } from '@svg/check.svg';
import { Icon } from 'antd';
import { inject, observer } from 'mobx-react';
import * as React from 'react';
import { Link } from 'react-router-dom';

import SourceIcon from '../icons/sourceIcon';
import EmptySourceCard from './emptySourceCard';
import { Content, EnabledText, StyledCard } from './styles';

export interface ISourceCardProps {
  sourceList?: ISourceStore[];
  source: ISourceStore | null;
  sourceDefinitionsListStore?: ISourceDefinitionsListStore;
  onMouseEnter?: (source: any) => void;
  onMouseLeave?: (source: any) => void;
  onDelete?: (source: any) => void;
}

export interface ISourceCardState {
  copied: boolean;
}

@inject('sourceDefinitionsListStore')
@observer
export default class SourceCard extends React.Component<
  ISourceCardProps,
  ISourceCardState
> {
  constructor(props: ISourceCardProps) {
    super(props);

    this.state = {
      copied: false,
    };
  }

  onMouseEnter = () => {
    this.props.onMouseEnter!(this.props.source);
  };

  onMouseLeave = () => {
    this.props.onMouseLeave!(this.props.source);
  };

  copyText = (event: any) => {
    const { source } = this.props;
    event.preventDefault();
    navigator.clipboard.writeText(source!.writeKey);
    this.setState({ copied: true });
    setTimeout(() => {
      this.setState({ copied: false });
    }, 1000);
  };

  deleteSource = (e: any) => {
    e.preventDefault();
    this.props.onDelete!(this.props.source);
  };

  public render() {
    const { source } = this.props;
    const { copied } = this.state;
    if (source === null) {
      return <EmptySourceCard />;
    } else {
      return (
        <Link to={`/sources/${source.id}`}>
          <StyledCard
            id={`source-${source.id}`}
            onMouseEnter={this.onMouseEnter}
            onMouseLeave={this.onMouseLeave}
          >
            <div style={{ flexShrink: 0 }}>
              <SourceIcon
                source={source.sourceDef.name}
                height={theme.iconSize.large}
                width={theme.iconSize.large}
              />
            </div>
            <Content>
              <Flex flexDirection="row" spaceBetween>
                <LabelDiv>{source.name}</LabelDiv>
              </Flex>
              {source.enabled && (
                <Flex flexDirection="row" spaceBetween>
                  <div>
                    <Check />
                    <EnabledText color={theme.color.green}>Enabled</EnabledText>
                  </div>
                  {copied ? (
                    <Text color={theme.color.grey300}>Write key copied</Text>
                  ) : null}
                </Flex>
              )}
              {!source.enabled && (
                <>
                  <Text color={theme.color.grey300}>Disabled</Text>
                </>
              )}
              <Flex
                spaceBetween
                className="m-h-sm"
                onClick={this.copyText}
                title="click to copy"
                style={{ minWidth: '300px' }}
              >
                <Text color={theme.color.grey300}>
                  Write key {source.writeKey}
                </Text>
                <div className="p-l-xs">
                  <Icon type="copy" style={{ color: theme.color.grey300 }} />
                </div>
              </Flex>
            </Content>
            <Flex
              flexDirection="row"
              alignItems="flex-end"
              style={{
                width: '1px',
                paddingRight: '35px',
                backgroundColor: 'red',
              }}
              id={`fake-source-${source!.id}`}
            />
          </StyledCard>
        </Link>
      );
    }
  }
}
